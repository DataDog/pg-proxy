package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTxStatus(t *testing.T) {
	assert := assert.New(t)

	s, err := TxStatusFromByte('I')
	assert.NoError(err)
	assert.Equal(Idle, s)
	s, err = TxStatusFromByte('T')
	assert.NoError(err)
	assert.Equal(InTransaction, s)
	s, err = TxStatusFromByte('E')
	assert.NoError(err)
	assert.Equal(InErroredTransaction, s)
	_, err = TxStatusFromByte('?')
	assert.ErrorContains(err, "unexpected TxStatus: ?")
}
