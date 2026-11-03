package envconfig

import (
	"errors"
	"fmt"
	"os"
	"strconv"
)

type Reader struct {
	errs []error
}

func (r *Reader) String(name string) string {
	value := os.Getenv(name)
	if value == "" {
		r.errs = append(r.errs, fmt.Errorf("%s environment variable is required", name))
	}
	return value
}

func (r *Reader) Int(name string) int {
	value, err := strconv.Atoi(os.Getenv(name))
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s environment variable is required and must be a number", name))
	}
	return value
}

func (r *Reader) Err() error {
	return errors.Join(r.errs...)
}
