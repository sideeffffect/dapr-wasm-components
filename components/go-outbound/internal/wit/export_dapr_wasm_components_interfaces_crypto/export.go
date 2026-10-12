package export_dapr_wasm_components_interfaces_crypto

import (
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_crypto"
	witTypes "go.bytecodealliance.org/pkg/wit/types"
	"runtime"
)

type Encryptor struct {
	pinner runtime.Pinner
	handle int32
}

type Decryptor struct {
	pinner runtime.Pinner
	handle int32
}

type CryptoOutput struct {
	pinner runtime.Pinner
	handle int32
}

func Encrypt(_componentName string, _data []uint8, _options dapr_wasm_components_interfaces_crypto.EncryptOptions) witTypes.Result[[]uint8, dapr_wasm_components_interfaces_crypto.EncryptError] {
	panic("not implemented")
}

func Decrypt(_componentName string, _data []uint8, _keyName witTypes.Option[string]) witTypes.Result[[]uint8, dapr_wasm_components_interfaces_crypto.DecryptError] {
	panic("not implemented")
}

func MakeEncryptor(_componentName string, _options dapr_wasm_components_interfaces_crypto.EncryptOptions) *Encryptor {
	panic("not implemented")
}

func (self *Encryptor) Write(_chunk []uint8) witTypes.Result[witTypes.Unit, dapr_wasm_components_interfaces_crypto.EncryptError] {
	panic("not implemented")
}

func EncryptorFinish(_this *Encryptor) witTypes.Result[*CryptoOutput, dapr_wasm_components_interfaces_crypto.EncryptError] {
	panic("not implemented")
}

func MakeDecryptor(_componentName string, _keyName witTypes.Option[string]) *Decryptor {
	panic("not implemented")
}

func (self *Decryptor) Write(_chunk []uint8) witTypes.Result[witTypes.Unit, dapr_wasm_components_interfaces_crypto.DecryptError] {
	panic("not implemented")
}

func DecryptorFinish(_this *Decryptor) witTypes.Result[*CryptoOutput, dapr_wasm_components_interfaces_crypto.DecryptError] {
	panic("not implemented")
}

func (self *CryptoOutput) Read(_maxLen uint64) witTypes.Option[[]uint8] {
	panic("not implemented")
}

func (self *Encryptor) OnDrop()    {}
func (self *Decryptor) OnDrop()    {}
func (self *CryptoOutput) OnDrop() {}
