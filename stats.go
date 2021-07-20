package segmentio

import (
	"context"
	"fmt"
	"time"

	kafka "github.com/segmentio/kafka-go"
	"github.com/unistack-org/micro/v3/meter"
)

func readerStats(ctx context.Context, r *kafka.Reader, td time.Duration, m meter.Meter) {
	ticker := time.NewTicker(td)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("done reader stats\n")
			return
		case <-ticker.C:
			if r == nil {
				return
			}
			rstats := r.Stats()
			labels := []string{"topic", rstats.Topic, "partition", rstats.Partition, "client_id", rstats.ClientID}

			m.Counter("broker_reader_dial_count", labels...).Add(int(rstats.Dials))
			m.Counter("broker_reader_fetch_count", labels...).Add(int(rstats.Fetches))
			m.Counter("broker_reader_message_count", labels...).Add(int(rstats.Messages))
			m.Counter("broker_reader_message_bytes", labels...).Add(int(rstats.Bytes))
			m.Counter("broker_reader_rebalance_count", labels...).Add(int(rstats.Rebalances))
			m.Counter("broker_reader_timeout_count", labels...).Add(int(rstats.Timeouts))
			m.Counter("broker_reader_error", labels...).Add(int(rstats.Errors))

			/*
				m.Counter("broker_reader_dial_seconds_avg", labels...).Add(uint64(rstats.DialTime.Avg))
				m.Counter("broker_reader_dial_seconds_min", labels...).Add(uint64(rstats.DialTime.Min))
				m.Counter("broker_reader_dial_seconds_max", labels...).Add(uint64(rstats.DialTime.Max))
				m.Counter("broker_reader_read_seconds_avg", labels...).Add(uint64(rstats.ReadTime.Avg))
				m.Counter("broker_reader_read_seconds_min", labels...).Add(uint64(rstats.ReadTime.Min))
				m.Counter("broker_reader_read_seconds_max", labels...).Add(uint64(rstats.ReadTime.Max))
				m.Counter("broker_reader_wait_seconds_avg", labels...).Add(uint64(rstats.WaitTime.Avg))
				m.Counter("broker_reader_wait_seconds_min", labels...).Add(uint64(rstats.WaitTime.Min))
				m.Counter("broker_reader_wait_seconds_max", labels...).Add(uint64(rstats.WaitTime.Max))
			*/
			/*
				m.Counter("broker_reader_fetch_size_avg", labels...).Add(uint64(rstats.FetchSize.Avg))
				m.Counter("broker_reader_fetch_size_min", labels...).Set(uint64(rstats.FetchSize.Min))
				m.Counter("broker_reader_fetch_size_max", labels...).Set(uint64(rstats.FetchSize.Max))
				m.Counter("broker_reader_fetch_bytes_avg", labels...).Set(uint64(rstats.FetchBytes.Avg))
				m.Counter("broker_reader_fetch_bytes_min", labels...).Set(uint64(rstats.FetchBytes.Min))
				m.Counter("broker_reader_fetch_bytes_max", labels...).Set(uint64(rstats.FetchBytes.Max))
			*/

			m.Counter("broker_reader_offset", labels...).Set(uint64(rstats.Offset))
			m.Counter("broker_reader_lag", labels...).Set(uint64(rstats.Lag))
			m.Counter("broker_reader_fetch_bytes_min", labels...).Set(uint64(rstats.MinBytes))
			m.Counter("broker_reader_fetch_bytes_max", labels...).Set(uint64(rstats.MaxBytes))
			m.Counter("broker_reader_fetch_wait_max", labels...).Set(uint64(rstats.MaxWait))
			m.Counter("broker_reader_queue_length", labels...).Set(uint64(rstats.QueueLength))
			m.Counter("broker_reader_queue_capacity", labels...).Set(uint64(rstats.QueueCapacity))
		}
	}
}

func writerStats(ctx context.Context, w *kafka.Writer, td time.Duration, m meter.Meter) {
	ticker := time.NewTicker(td)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if w == nil {
				return
			}
			wstats := w.Stats()
			labels := []string{}

			m.Counter("broker_writer_write_count", labels...).Add(int(wstats.Writes))
			m.Counter("broker_writer_message_count", labels...).Add(int(wstats.Messages))
			m.Counter("broker_writer_message_bytes", labels...).Add(int(wstats.Bytes))
			m.Counter("broker_writer_error_count", labels...).Add(int(wstats.Errors))

			/*
				m.Counter("broker_writer_batch_seconds_avg", labels...).Set(uint64(wstats.BatchTime.Avg))
				m.Counter("broker_writer_batch_seconds_min", labels...).Set(uint64(wstats.BatchTime.Min))
				m.Counter("broker_writer_batch_seconds_max", labels...).Set(uint64(wstats.BatchTime.Max))
				m.Counter("broker_writer_write_seconds_avg", labels...).Set(uint64(wstats.WriteTime.Avg))
				m.Counter("broker_writer_write_seconds_min", labels...).Set(uint64(wstats.WriteTime.Min))
				m.Counter("broker_writer_write_seconds_max", labels...).Set(uint64(wstats.WriteTime.Max))
				m.Counter("broker_writer_wait_seconds_avg", labels...).Set(uint64(wstats.WaitTime.Avg))
				m.Counter("broker_writer_wait_seconds_min", labels...).Set(uint64(wstats.WaitTime.Min))
				m.Counter("broker_writer_wait_seconds_max", labels...).Set(uint64(wstats.WaitTime.Max))

				m.Counter("broker_writer_retries_count_avg", labels...).Set(uint64(wstats.Retries.Avg))
				m.Counter("broker_writer_retries_count_min", labels...).Set(uint64(wstats.Retries.Min))
				m.Counter("broker_writer_retries_count_max", labels...).Set(uint64(wstats.Retries.Max))
				m.Counter("broker_writer_batch_size_avg", labels...).Set(uint64(wstats.BatchSize.Avg))
				m.Counter("broker_writer_batch_size_min", labels...).Set(uint64(wstats.BatchSize.Min))
				m.Counter("broker_writer_batch_size_max", labels...).Set(uint64(wstats.BatchSize.Max))
				m.Counter("broker_writer_batch_bytes_avg", labels...).Set(uint64(wstats.BatchBytes.Avg))
				m.Counter("broker_writer_batch_bytes_min", labels...).Set(uint64(wstats.BatchBytes.Min))
				m.Counter("broker_writer_batch_bytes_max", labels...).Set(uint64(wstats.BatchBytes.Max))
			*/

			m.Counter("broker_writer_attempts_max", labels...).Set(uint64(wstats.MaxAttempts))
			m.Counter("broker_writer_batch_max", labels...).Set(uint64(wstats.MaxBatchSize))
			m.Counter("broker_writer_batch_timeout", labels...).Set(uint64(wstats.BatchTimeout))
			m.Counter("broker_writer_read_timeout", labels...).Set(uint64(wstats.ReadTimeout))
			m.Counter("broker_writer_write_timeout", labels...).Set(uint64(wstats.WriteTimeout))
			m.Counter("broker_writer_acks_required", labels...).Set(uint64(wstats.RequiredAcks))
			if wstats.Async {
				m.Counter("broker_writer_async", labels...).Set(1)
			} else {
				m.Counter("broker_writer_async", labels...).Set(0)
			}
		}
	}
}
