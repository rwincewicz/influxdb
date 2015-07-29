package amqp

import (
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/influxdb/meta"
	"github.com/influxdb/influxdb/tsdb"
	"github.com/streadway/amqp"
)

const leaderWaitTimeout = 30 * time.Second

// pointsWriter is an internal interface to make testing easier.
type pointsWriter interface {
	WritePoints(p *cluster.WritePointsRequest) error
}

// metaStore is an internal interface to make testing easier.
type metaStore interface {
	WaitForLeader(d time.Duration) error
	CreateDatabaseIfNotExists(name string) (*meta.DatabaseInfo, error)
}

// Service represents a UDP server which receives metrics in collectd's binary
// protocol and stores them in InfluxDB.
type Service struct {
	Config       *Config
	MetaStore    metaStore
	PointsWriter pointsWriter
	Logger       *log.Logger

	wg       sync.WaitGroup
	err      chan error
	stop     chan struct{}
	messages chan amqp.Delivery
	batcher  *tsdb.PointBatcher

	conn amqp.Connection
	c    amqp.Channel
}

// NewService returns a new instance of the amqp service.
func NewService(c Config) *Service {
	s := &Service{
		Config: &c,
		Logger: log.New(os.Stderr, "[amqp] ", log.LstdFlags),
		err:    make(chan error),
	}

	return s
}

// Open starts the service.
func (s *Service) Open() error {
	if s.Config.AMQPAddress == "" {
		return fmt.Errorf("AMQP address is blank")
	} else if s.Config.AMQPPort == "" {
		return fmt.Errorf("AMQP port is blank")
	} else if s.PointsWriter == nil {
		return fmt.Errorf("PointsWriter is nil")
	}

	if err := s.MetaStore.WaitForLeader(leaderWaitTimeout); err != nil {
		s.Logger.Printf("failed to detect a cluster leader: %s", err.Error())
		return err
	}

	if _, err := s.MetaStore.CreateDatabaseIfNotExists(s.Config.Database); err != nil {
		s.Logger.Printf("failed to ensure target database %s exists: %s", s.Config.Database, err.Error())
		return err
	}

	connectUrl := fmt.Sprintf("amqp://%s:%s@%s:%s/%s", s.Config.User, s.Config.Pass, s.Config.AMQPAddress, s.Config.AMQPPort, s.Config.VHost)
	s.conn, err = amqp.Dial(connectUrl)
	if err != nil {
		s.Logger.Printf("could not connect to AMQP server: %s", err)
	}

	s.c, err = s.conn.Channel()
	if err != nil {
		s.Logger.Printf("could not create channel on AMQP server: %s", err)
	}

	err = c.ExchangeDeclare(s.Config.Exchange, "topic", true, false, false, false, nil)
	if err != nil {
		s.Logger.Printf("exchange.declare: %s", err)
	}

	_, err = c.QueueDeclare(s.Config.Queue, true, false, false, false, nil)
	if err != nil {
		s.Logger.Printf("queue.declare: %v", err)
	}

	err = c.QueueBind(s.Config.Queue, s.Config.Key, s.Config.Exchange, false, nil)
	if err != nil {
		s.Logger.Printf("queue.bind: %v", err)
	}

	s.messages, err = s.c.Consume(s.Config.Queue, "InfluxDB", false, false, false, false, nil)
	if err != nil {
		s.Logger.Printf("queue.consume: %v", err)
	}

	// Start the points batcher.
	s.batcher = tsdb.NewPointBatcher(s.Config.BatchSize, time.Duration(s.Config.BatchDuration))
	s.batcher.Start()

	// Create channel and wait group for signalling goroutines to stop.
	s.stop = make(chan struct{})
	s.wg.Add(2)

	// Start goroutines that process collectd packets.
	go s.serve()
	go s.writePoints()

	s.Logger.Println("AMQP listener started")

	return nil
}

// Close stops the service.
func (s *Service) Close() error {
	// Close the connection, and wait for the goroutine to exit.
	if s.stop != nil {
		close(s.stop)
	}
	if s.conn != nil {
		s.conn.Close()
	}
	if s.batcher != nil {
		s.batcher.Stop()
	}
	s.wg.Wait()

	// Release all remaining resources.
	s.stop = nil
	s.conn = nil
	s.batcher = nil
	s.Logger.Println("AMQP listener closed")
	return nil
}

// SetLogger sets the internal logger to the logger passed in.
func (s *Service) SetLogger(l *log.Logger) {
	s.Logger = l
}

// Err returns a channel for fatal errors that occur on go routines.
func (s *Service) Err() chan error { return s.err }

func (s *Service) serve() {
	defer s.wg.Done()

	for {
		select {
		case <-s.stop:
			// We closed the connection, time to go.
			return
		default:
			// Keep processing.
		}

		for message = range s.messages {
			s.Logger.Printf("Message: %v", message)
		}
	}
}

func (s *Service) writePoints() {
	defer s.wg.Done()

	for {
		select {
		case <-s.stop:
			return
		case batch := <-s.batcher.Out():
			req := &cluster.WritePointsRequest{
				Database:         s.Config.Database,
				RetentionPolicy:  s.Config.RetentionPolicy,
				ConsistencyLevel: cluster.ConsistencyLevelAny,
				Points:           batch,
			}
			if err := s.PointsWriter.WritePoints(req); err != nil {
				s.Logger.Printf("failed to write batch: %s", err)
				continue
			}
		}
	}
}

// Unmarshal translates a collectd packet into InfluxDB data points.
func Unmarshal(message amqp.Delivery) []tsdb.Point {
	// Prefer high resolution timestamp.
	var timestamp time.Time
	if packet.TimeHR > 0 {
		// TimeHR is "near" nanosecond measurement, but not exactly nanasecond time
		// Since we store time in microseconds, we round here (mostly so tests will work easier)
		sec := packet.TimeHR >> 30
		// Shifting, masking, and dividing by 1 billion to get nanoseconds.
		nsec := ((packet.TimeHR & 0x3FFFFFFF) << 30) / 1000 / 1000 / 1000
		timestamp = time.Unix(int64(sec), int64(nsec)).UTC().Round(time.Microsecond)
	} else {
		// If we don't have high resolution time, fall back to basic unix time
		timestamp = time.Unix(int64(packet.Time), 0).UTC()
	}

	var points []tsdb.Point
	for i := range packet.Values {
		name := fmt.Sprintf("%s_%s", packet.Plugin, packet.Values[i].Name)
		tags := make(map[string]string)
		fields := make(map[string]interface{})

		fields["value"] = packet.Values[i].Value

		if packet.Hostname != "" {
			tags["host"] = packet.Hostname
		}
		if packet.PluginInstance != "" {
			tags["instance"] = packet.PluginInstance
		}
		if packet.Type != "" {
			tags["type"] = packet.Type
		}
		if packet.TypeInstance != "" {
			tags["type_instance"] = packet.TypeInstance
		}
		p := tsdb.NewPoint(name, tags, fields, timestamp)

		points = append(points, p)
	}
	return points
}

// assert will panic with a given formatted message if the given condition is false.
func assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assert failed: "+msg, v...))
	}
}
