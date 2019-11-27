/**
 * @Description: 
 * @Version: 1.0.0
 * @Author: liteng
 * @Date: 2019-11-27 19:41
 */

package config

import (
	"fmt"
	"testing"
)

func TestLoadConfig(t *testing.T) {

	config, err := LoadConfig(CONFIG_FILE_PATH)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(*config)
}