package clients

import "fmt"

func Producer(cfgFile string) {
	fmt.Printf("Hi this should start a producer %s\n", cfgFile)
}
