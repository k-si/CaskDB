package util

import "os"

func CheckAndMakeDir(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err = os.MkdirAll(path, os.ModePerm); err != nil {
			return err
		}
	}
	return nil
}

