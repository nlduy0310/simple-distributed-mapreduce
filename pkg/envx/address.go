package envx

import "errors"

type Address string

func AddressParserFunc(s string) (interface{}, error) {
	if len(s) == 0 {
		return nil, errors.New("address must not be empty")
	}
	// ...

	return Address(s), nil
}
