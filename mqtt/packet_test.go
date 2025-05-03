package mqtt

import (
	"bytes"
	"reflect"
	"testing"
)

func TestReadFixedHeader(t *testing.T) {
	type args struct {
		headerBytes []byte
	}
	tests := []struct {
		name    string
		args    args
		want    FixedHeader
		wantErr error
	}{
		{
			"CONNECT",
			args{[]byte{0x10, 0x00}},
			FixedHeader{CONNECT, 0, 0},
			nil,
		},
		{
			"CONNACK",
			args{[]byte{0x20, 0x00}},
			FixedHeader{CONNACK, 0, 0},
			nil,
		},
		{
			"PUBLISH",
			args{[]byte{0x3F, 0x00}},
			FixedHeader{PUBLISH, 0x0F, 0},
			nil,
		},
		{
			"PUBACK",
			args{[]byte{0x40, 0x00}},
			FixedHeader{PUBACK, 0, 0},
			nil,
		},
		{
			"PUBREC",
			args{[]byte{0x50, 0x00}},
			FixedHeader{PUBREC, 0, 0},
			nil,
		},
		{
			"PUBREL",
			args{[]byte{0x62, 0x00}},
			FixedHeader{PUBREL, 0x02, 0},
			nil,
		},
		{
			"PUBCOMP",
			args{[]byte{0x70, 0x00}},
			FixedHeader{PUBCOMP, 0, 0},
			nil,
		},
		{
			"SUBSCRIBE",
			args{[]byte{0x82, 0x00}},
			FixedHeader{SUBSCRIBE, 0x02, 0},
			nil,
		},
		{
			"SUBACK",
			args{[]byte{0x90, 0x00}},
			FixedHeader{SUBACK, 0, 0},
			nil,
		},
		{
			"UNSUBSCRIBE",
			args{[]byte{0xA2, 0x00}},
			FixedHeader{UNSUBSCRIBE, 0x02, 0},
			nil,
		},
		{
			"UNSUBACK",
			args{[]byte{0xB0, 0x00}},
			FixedHeader{UNSUBACK, 0, 0},
			nil,
		},
		{
			"PINGREQ",
			args{[]byte{0xC0, 0x00}},
			FixedHeader{PINGREQ, 0, 0},
			nil,
		},
		{
			"PINGRESP",
			args{[]byte{0xD0, 0x00}},
			FixedHeader{PINGRESP, 0, 0},
			nil,
		},
		{
			"DISCONNECT",
			args{[]byte{0xE0, 0x00}},
			FixedHeader{DISCONNECT, 0, 0},
			nil,
		},
		{
			"AUTH",
			args{[]byte{0xF0, 0x00}},
			FixedHeader{AUTH, 0, 0},
			nil,
		},
		{
			"Reserved",
			args{[]byte{0x00, 0x00}},
			FixedHeader{},
			MalformedPacket,
		},
		{
			"Missing0Flag",
			args{[]byte{0x11, 0x00}},
			FixedHeader{},
			MalformedPacket,
		},
		{
			"Missing2Flag",
			args{[]byte{0x60, 0x00}},
			FixedHeader{},
			MalformedPacket,
		},
		{
			"VariableLengthTooLong",
			args{[]byte{0x10, 0x80, 0x80, 0x80, 0x80, 0x01}},
			FixedHeader{},
			MalformedPacket,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bytes.NewReader(tt.args.headerBytes)
			got, err := ReadFixedHeader(r)
			if err != tt.wantErr {
				t.Errorf("ReadFixedHeader() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReadFixedHeader() = %v, want %v", got, tt.want)
			}
		})
	}
}
