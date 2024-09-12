// Code generated by mockery v2.32.4. DO NOT EDIT.

package common

import mock "github.com/stretchr/testify/mock"

// MockMessageID is an autogenerated mock type for the MessageID type
type MockMessageID struct {
	mock.Mock
}

type MockMessageID_Expecter struct {
	mock *mock.Mock
}

func (_m *MockMessageID) EXPECT() *MockMessageID_Expecter {
	return &MockMessageID_Expecter{mock: &_m.Mock}
}

// AtEarliestPosition provides a mock function with given fields:
func (_m *MockMessageID) AtEarliestPosition() bool {
	ret := _m.Called()

	var r0 bool
	if rf, ok := ret.Get(0).(func() bool); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}

// MockMessageID_AtEarliestPosition_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'AtEarliestPosition'
type MockMessageID_AtEarliestPosition_Call struct {
	*mock.Call
}

// AtEarliestPosition is a helper method to define mock.On call
func (_e *MockMessageID_Expecter) AtEarliestPosition() *MockMessageID_AtEarliestPosition_Call {
	return &MockMessageID_AtEarliestPosition_Call{Call: _e.mock.On("AtEarliestPosition")}
}

func (_c *MockMessageID_AtEarliestPosition_Call) Run(run func()) *MockMessageID_AtEarliestPosition_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *MockMessageID_AtEarliestPosition_Call) Return(_a0 bool) *MockMessageID_AtEarliestPosition_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockMessageID_AtEarliestPosition_Call) RunAndReturn(run func() bool) *MockMessageID_AtEarliestPosition_Call {
	_c.Call.Return(run)
	return _c
}

// Equal provides a mock function with given fields: msgID
func (_m *MockMessageID) Equal(msgID []byte) (bool, error) {
	ret := _m.Called(msgID)

	var r0 bool
	var r1 error
	if rf, ok := ret.Get(0).(func([]byte) (bool, error)); ok {
		return rf(msgID)
	}
	if rf, ok := ret.Get(0).(func([]byte) bool); ok {
		r0 = rf(msgID)
	} else {
		r0 = ret.Get(0).(bool)
	}

	if rf, ok := ret.Get(1).(func([]byte) error); ok {
		r1 = rf(msgID)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockMessageID_Equal_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Equal'
type MockMessageID_Equal_Call struct {
	*mock.Call
}

// Equal is a helper method to define mock.On call
//   - msgID []byte
func (_e *MockMessageID_Expecter) Equal(msgID interface{}) *MockMessageID_Equal_Call {
	return &MockMessageID_Equal_Call{Call: _e.mock.On("Equal", msgID)}
}

func (_c *MockMessageID_Equal_Call) Run(run func(msgID []byte)) *MockMessageID_Equal_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].([]byte))
	})
	return _c
}

func (_c *MockMessageID_Equal_Call) Return(_a0 bool, _a1 error) *MockMessageID_Equal_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockMessageID_Equal_Call) RunAndReturn(run func([]byte) (bool, error)) *MockMessageID_Equal_Call {
	_c.Call.Return(run)
	return _c
}

// LessOrEqualThan provides a mock function with given fields: msgID
func (_m *MockMessageID) LessOrEqualThan(msgID []byte) (bool, error) {
	ret := _m.Called(msgID)

	var r0 bool
	var r1 error
	if rf, ok := ret.Get(0).(func([]byte) (bool, error)); ok {
		return rf(msgID)
	}
	if rf, ok := ret.Get(0).(func([]byte) bool); ok {
		r0 = rf(msgID)
	} else {
		r0 = ret.Get(0).(bool)
	}

	if rf, ok := ret.Get(1).(func([]byte) error); ok {
		r1 = rf(msgID)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockMessageID_LessOrEqualThan_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'LessOrEqualThan'
type MockMessageID_LessOrEqualThan_Call struct {
	*mock.Call
}

// LessOrEqualThan is a helper method to define mock.On call
//   - msgID []byte
func (_e *MockMessageID_Expecter) LessOrEqualThan(msgID interface{}) *MockMessageID_LessOrEqualThan_Call {
	return &MockMessageID_LessOrEqualThan_Call{Call: _e.mock.On("LessOrEqualThan", msgID)}
}

func (_c *MockMessageID_LessOrEqualThan_Call) Run(run func(msgID []byte)) *MockMessageID_LessOrEqualThan_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].([]byte))
	})
	return _c
}

func (_c *MockMessageID_LessOrEqualThan_Call) Return(_a0 bool, _a1 error) *MockMessageID_LessOrEqualThan_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockMessageID_LessOrEqualThan_Call) RunAndReturn(run func([]byte) (bool, error)) *MockMessageID_LessOrEqualThan_Call {
	_c.Call.Return(run)
	return _c
}

// Serialize provides a mock function with given fields:
func (_m *MockMessageID) Serialize() []byte {
	ret := _m.Called()

	var r0 []byte
	if rf, ok := ret.Get(0).(func() []byte); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]byte)
		}
	}

	return r0
}

// MockMessageID_Serialize_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Serialize'
type MockMessageID_Serialize_Call struct {
	*mock.Call
}

// Serialize is a helper method to define mock.On call
func (_e *MockMessageID_Expecter) Serialize() *MockMessageID_Serialize_Call {
	return &MockMessageID_Serialize_Call{Call: _e.mock.On("Serialize")}
}

func (_c *MockMessageID_Serialize_Call) Run(run func()) *MockMessageID_Serialize_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *MockMessageID_Serialize_Call) Return(_a0 []byte) *MockMessageID_Serialize_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockMessageID_Serialize_Call) RunAndReturn(run func() []byte) *MockMessageID_Serialize_Call {
	_c.Call.Return(run)
	return _c
}

// NewMockMessageID creates a new instance of MockMessageID. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockMessageID(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockMessageID {
	mock := &MockMessageID{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
