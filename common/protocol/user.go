package protocol

import (
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/serial"
)

func (u *User) GetTypedAccount() (Account, error) {
	if u.GetAccount() == nil {
		return nil, errors.New("Account is missing").AtWarning()
	}

	rawAccount, err := u.Account.GetInstance()
	if err != nil {
		return nil, err
	}
	if asAccount, ok := rawAccount.(AsAccount); ok {
		return asAccount.AsAccount()
	}
	if account, ok := rawAccount.(Account); ok {
		return account, nil
	}
	return nil, errors.New("Unknown account type: ", u.Account.Type)
}

func (u *User) ToMemoryUser() (*MemoryUser, error) {
	account, err := u.GetTypedAccount()
	if err != nil {
		return nil, err
	}
	return &MemoryUser{
		Account: account,
		Email:   u.Email,
		Level:   u.Level,
	}, nil
}

func ToProtoUser(mu *MemoryUser) *User {
	if mu == nil {
		return nil
	}
	return &User{
		Account: serial.ToTypedMessage(mu.Account.ToProto()),
		Email:   mu.Email,
		Level:   mu.Level,
	}
}

// MemoryUser is a parsed form of User, to reduce number of parsing of Account proto.
type MemoryUser struct {
	// Account is the parsed account of the protocol.
	Account Account
	Email   string
	Level   uint32
}

// UserIDer is implemented by protocol accounts that expose a stable user id (for example a VLESS UUID).
type UserIDer interface {
	UserID() string
}

// Identifiers returns email and protocol user id, skipping empty values.
func (u *MemoryUser) Identifiers() []string {
	if u == nil {
		return nil
	}
	ids := make([]string, 0, 2)
	if u.Email != "" {
		ids = append(ids, u.Email)
	}
	if ider, ok := u.Account.(UserIDer); ok {
		if id := ider.UserID(); id != "" && id != u.Email {
			ids = append(ids, id)
		}
	}
	return ids
}
