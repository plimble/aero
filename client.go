package aero

import (
	"github.com/aerospike/aerospike-client-go"
	"github.com/plimble/utils/errors2"
	"github.com/tinylib/msgp/msgp"
)

const (
	STRING      = aerospike.STRING
	NUMERIC     = aerospike.NUMERIC
	CREATE_ONLY = aerospike.CREATE_ONLY
	WRITE       = aerospike.WRITE
)

type Client struct {
	*aerospike.Client
}

func NewClient(hostname string, port int) *Client {
	client, err := aerospike.NewClient(hostname, port)
	errors2.Panic(err)

	return &Client{
		Client: client,
	}
}

func NewBin(name string, val interface{}) *aerospike.Bin {
	return aerospike.NewBin(name, val)
}

func NewKey(ns, set string, key interface{}) (*aerospike.Key, error) {
	k, err := aerospike.NewKey(ns, set, key)
	if err != nil {
		return nil, errors2.NewInternal(err.Error())
	}

	return k, nil
}

func NewArrayKeys(size int) []*aerospike.Key {
	return make([]*aerospike.Key, size)
}

func NewWritePolicy(generation, expiration int32) *aerospike.WritePolicy {
	return aerospike.NewWritePolicy(generation, expiration)
}

func NewPolicy() *aerospike.BasePolicy {
	return aerospike.NewPolicy()
}

func NewQueryPolicy() *aerospike.QueryPolicy {
	return aerospike.NewQueryPolicy()
}

func NewStatement(ns, set string, binNames ...string) *aerospike.Statement {
	return aerospike.NewStatement(ns, set, binNames...)
}

func MarshalMsgPack(v msgp.Marshaler) ([]byte, error) {
	b, err := v.MarshalMsg(nil)
	if err != nil {
		return nil, errors2.NewInternal(err.Error())
	}

	return b, nil
}

func UnmarshalMsgPack(data []byte, v msgp.Unmarshaler) error {
	if _, err := v.UnmarshalMsg(data); err != nil {
		return errors2.NewInternal(err.Error())
	}

	return nil
}

func NewEqualFilter(binName string, value interface{}) *aerospike.Filter {
	return aerospike.NewEqualFilter(binName, value)
}

func NewRangeFilter(binName string, begin int64, end int64) *aerospike.Filter {
	return aerospike.NewRangeFilter(binName, begin, end)
}

func (c *Client) PutBins(policy *aerospike.WritePolicy, ns, set string, key interface{}, bin ...*aerospike.Bin) error {
	k, err := aerospike.NewKey(ns, set, key)
	if err != nil {
		return errors2.NewInternal(err.Error())
	}

	return errPut(c.Client.PutBins((*aerospike.WritePolicy)(policy), k, bin...))
}

func (c *Client) Get(policy *aerospike.BasePolicy, ns, set string, key interface{}, binNames ...string) (*aerospike.Record, error) {
	k, err := aerospike.NewKey(ns, set, key)
	if err != nil {
		return nil, errors2.NewInternal(err.Error())
	}

	rec, err := c.Client.Get(policy, k, binNames...)
	if err := errGet(rec, err); err != nil {
		return nil, err
	}

	return rec, nil
}

func (c *Client) Delete(policy *aerospike.WritePolicy, ns, set string, key interface{}) error {
	k, err := aerospike.NewKey(ns, set, key)
	if err != nil {
		return errors2.NewInternal(err.Error())
	}

	return errDel(c.Client.Delete(policy, k))
}

func (c *Client) Exists(policy *aerospike.BasePolicy, ns, set string, key interface{}) (bool, error) {
	k, err := aerospike.NewKey(ns, set, key)
	if err != nil {
		return false, errors2.NewInternal(err.Error())
	}

	exist, err := c.Client.Exists(policy, k)
	if err != nil {
		return false, errors2.NewInternal(err.Error())
	}

	return exist, nil
}

func (c *Client) BatchGet(policy *aerospike.BasePolicy, keys []*aerospike.Key, binNames ...string) ([]*aerospike.Record, error) {
	recs, err := c.Client.BatchGet(policy, keys, binNames...)
	if err != nil {
		return nil, errors2.NewInternal(err.Error())
	}

	return recs, nil
}

func (c *Client) Query(policy *aerospike.QueryPolicy, statement *aerospike.Statement) (*aerospike.Recordset, error) {
	rs, err := c.Client.Query(policy, statement)
	if err != nil {
		return nil, errors2.NewInternal(err.Error())
	}

	return rs, nil
}

func (c *Client) CreateIndex(
	policy *aerospike.WritePolicy,
	namespace string,
	setName string,
	indexName string,
	binName string,
	indexType aerospike.IndexType,
) (*aerospike.IndexTask, error) {
	t, err := c.Client.CreateIndex(policy, namespace, setName, indexName, binName, indexType)
	if err != nil {
		return nil, errIndex(err)
	}

	return t, nil
}
