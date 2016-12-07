/*
 * Copyright (c) 2016 TFG Co <backend@tfgco.com>
 * Author: TFG Co <backend@tfgco.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package worker

import (
	"strings"

	"github.com/jrallison/go-workers"
	"github.com/satori/go.uuid"
	"github.com/spf13/viper"
	"github.com/uber-go/zap"
)

// Worker is the struct that will configure workers
type Worker struct {
	Debug      bool
	Logger     zap.Logger
	ConfigPath string
	Config     *viper.Viper
}

// NewWorker returns a configured worker
func NewWorker(debug bool, l zap.Logger, configPath string) *Worker {
	worker := &Worker{
		Debug:      debug,
		Logger:     l,
		ConfigPath: configPath,
	}

	worker.configure()
	return worker
}

func (w *Worker) configure() {
	w.Config = viper.New()

	w.Config.SetConfigFile(w.ConfigPath)
	w.Config.SetEnvPrefix("marathon")
	w.Config.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	w.Config.AutomaticEnv()

	if err := w.Config.ReadInConfig(); err == nil {
		w.Logger.Info("Loaded config file.", zap.String("configFile", w.Config.ConfigFileUsed()))
	} else {
		panic(err)
	}
	w.loadConfigurationDefaults()
	w.configureRedis()
	w.configureWorkers()
}

func (w *Worker) loadConfigurationDefaults() {
	w.Config.SetDefault("workers.redis.server", "localhost:6379")
	w.Config.SetDefault("workers.redis.database", "0")
	w.Config.SetDefault("workers.redis.poolSize", "10")
	w.Config.SetDefault("workers.statsPort", 8081)
	w.Config.SetDefault("workers.concurrency", 10)
	w.Config.SetDefault("database.url", "postgres://localhost:5432/marathon?sslmode=disable")
}

func (w *Worker) configureRedis() {
	redisServer := w.Config.GetString("workers.redis.server")
	redisDatabase := w.Config.GetString("workers.redis.database")
	redisPoolsize := w.Config.GetString("workers.redis.poolSize")

	logger := w.Logger.With(
		zap.String("redisServer", redisServer),
		zap.String("redisDB", redisDatabase),
		zap.String("redisPoolsize", redisPoolsize),
	)

	logger.Info("connecting to workers redis")
	// unique process id for this instance of workers (for recovery of inprogress jobs on crash)
	redisProcessID := uuid.NewV4()

	workers.Configure(map[string]string{
		"server":   redisServer,
		"database": redisDatabase,
		"pool":     redisPoolsize,
		"process":  redisProcessID.String(),
	})
}

func (w *Worker) configureWorkers() {
	jobsConcurrency := w.Config.GetInt("workers.concurrency")
	b := GetBenchmarkWorker(w.Config.GetString("workers.redis.server"), w.Config.GetString("workers.redis.database"))
	c := NewCreateBatchesWorker(w.Config)
	//workers.Middleware.Append(&) TODO
	workers.Process("benchmark_worker", b.Process, jobsConcurrency)
	workers.Process("create_batches_worker", c.Process, jobsConcurrency)
}

// CreateBatchesJob creates a new CreateBatchesWorker job
func (w *Worker) CreateBatchesJob(jobID *[]string) (string, error) {
	return workers.Enqueue("create_batches_worker", "Add", jobID)
}

// Start starts the worker
func (w *Worker) Start() {
	jobsStatsPort := viper.GetInt("workers.statsPort")
	go workers.StatsServer(jobsStatsPort)
	workers.Run()
}
