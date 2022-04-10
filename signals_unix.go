// +build linux bsd darwin

package asynq

import (
	"os"
	"os/signal"

	"golang.org/x/sys/unix"
)

// waitForSignals waits for signals and handles them.
// It handles SIGTERM, SIGINT, and SIGTSTP.
// SIGTERM and SIGINT will signal the process to exit.
// SIGTSTP will signal the process to stop processing new tasks.
// waitForSignals等待并处理信号。
// 它处理SIGTERM, SIGINT和SIGTSTP。
// SIGTERM和SIGINT将发出退出的信号。
// SIGTSTP将向进程发出停止处理新任务的信号。
func (srv *Server) waitForSignals() {
	srv.logger.Info("Send signal TSTP to stop processing new tasks")
	srv.logger.Info("Send signal TERM or INT to terminate the process")

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, unix.SIGTERM, unix.SIGINT, unix.SIGTSTP)
	for {
		sig := <-sigs
		if sig == unix.SIGTSTP {
			srv.Stop()
			continue
		}
		break
	}
}

func (s *Scheduler) waitForSignals() {
	s.logger.Info("Send signal TERM or INT to stop the scheduler")
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, unix.SIGTERM, unix.SIGINT)
	<-sigs
}
