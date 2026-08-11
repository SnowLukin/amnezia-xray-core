package wireguard

import (
	"strings"
	"testing"
)

func TestWriteAmneziaParams(t *testing.T) {
	testCases := []struct {
		name   string
		params *AmneziaParameters
		expect string
	}{
		{
			name:   "nil parameters emit nothing",
			params: nil,
			expect: "",
		},
		{
			name: "empty values are skipped",
			params: &AmneziaParameters{
				Jc: "3",
				H1: "39131278",
			},
			expect: "jc=3\nh1=39131278\n",
		},
		{
			name: "full set keeps device-section order",
			params: &AmneziaParameters{
				Jc: "3", Jmin: "50", Jmax: "1000",
				S1: "20", S2: "78", S3: "0", S4: "0",
				H1: "39131278", H2: "832138185",
				H3: "1436957857", H4: "1635877746",
				I1: "<b 0xc0>", I2: "<r 8>", I3: "<rd 4>", I4: "<rc 4>", I5: "<t>",
			},
			expect: "jc=3\njmin=50\njmax=1000\n" +
				"s1=20\ns2=78\ns3=0\ns4=0\n" +
				"h1=39131278\nh2=832138185\nh3=1436957857\nh4=1635877746\n" +
				"i1=<b 0xc0>\ni2=<r 8>\ni3=<rd 4>\ni4=<rc 4>\ni5=<t>\n",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			var cfg strings.Builder
			writeAmneziaParams(&cfg, testCase.params)

			if cfg.String() != testCase.expect {
				t.Errorf("got %q, want %q", cfg.String(), testCase.expect)
			}
		})
	}
}
