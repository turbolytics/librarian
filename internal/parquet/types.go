package parquet

import (
	"fmt"
	"math/big"
	"strings"
)

func DecimalStringToInt(decimalStr string, precision int, scale int) (*big.Int, error) {
	// Handle the sign
	sign := 1
	if strings.HasPrefix(decimalStr, "-") {
		sign = -1
		decimalStr = decimalStr[1:]
	}

	// Split the integer and fractional parts
	parts := strings.Split(decimalStr, ".")
	if len(parts) > 2 {
		return nil, fmt.Errorf("invalid decimal string format")
	}

	integerPart := parts[0]
	fractionalPart := ""
	if len(parts) == 2 {
		fractionalPart = parts[1]
	}

	// Pad or truncate the fractional part to match the scale
	if len(fractionalPart) < scale {
		fractionalPart += strings.Repeat("0", scale-len(fractionalPart))
	} else if len(fractionalPart) > scale {
		fractionalPart = fractionalPart[:scale]
	}

	// Combine integer and fractional parts to form the unscaled integer string
	unscaledStr := integerPart + fractionalPart

	// Convert the unscaled string to a big.Int
	unscaled := new(big.Int)
	_, ok := unscaled.SetString(unscaledStr, 10)
	if !ok {
		return nil, fmt.Errorf("invalid number format")
	}

	// Apply the sign
	if sign == -1 {
		unscaled.Neg(unscaled)
	}

	return unscaled, nil
}

func StringToDECIMAL_BYTE_ARRAY(decimalStr string, precision int, scale int) ([]byte, error) {
	// Handle the sign
	sign := 1
	if strings.HasPrefix(decimalStr, "-") {
		sign = -1
		decimalStr = decimalStr[1:]
	}

	// Split the integer and fractional parts
	parts := strings.Split(decimalStr, ".")
	if len(parts) > 2 {
		return nil, fmt.Errorf("invalid decimal string format")
	}

	integerPart := parts[0]
	fractionalPart := ""
	if len(parts) == 2 {
		fractionalPart = parts[1]
	}

	// Pad or truncate the fractional part to match the scale
	if len(fractionalPart) < scale {
		fractionalPart += strings.Repeat("0", scale-len(fractionalPart))
	} else if len(fractionalPart) > scale {
		fractionalPart = fractionalPart[:scale]
	}

	// Combine integer and fractional parts to form the unscaled integer string
	unscaledStr := integerPart + fractionalPart

	// Convert the unscaled string to a big.Int
	unscaled := new(big.Int)
	_, ok := unscaled.SetString(unscaledStr, 10)
	if !ok {
		return nil, fmt.Errorf("invalid number format")
	}

	// Apply the sign
	if sign == -1 {
		unscaled.Neg(unscaled)
	}

	// Convert to two's complement big-endian byte slice
	byteSize := (precision + 1) / 2 // Approximate byte length based on precision
	bytes := unscaled.Bytes()

	// Pad to the required byte size
	paddedBytes := make([]byte, byteSize)
	copy(paddedBytes[byteSize-len(bytes):], bytes)

	// If negative, convert to two's complement
	if sign == -1 {
		for i := range paddedBytes {
			paddedBytes[i] = paddedBytes[i] ^ 0xff
		}
		// Add 1 to complete two's complement conversion
		for i := len(paddedBytes) - 1; i >= 0; i-- {
			paddedBytes[i]++
			if paddedBytes[i] != 0 {
				break
			}
		}
	}

	return paddedBytes, nil
}
