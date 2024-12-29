package postgres

import "testing"

func TestExtractPulicationNameFromQuery(t *testing.T) {
	tests := []struct {
		query   string
		want    string
		wantErr bool
	}{
		{
			query:   "CREATE PUBLICATION my_publication",
			want:    "my_publication",
			wantErr: false,
		},
		{
			query:   "CREATE PUBLICATION my_publication FOR TABLE users, departments, TABLES IN SCHEMA production;",
			want:    "my_publication",
			wantErr: false,
		},
		{
			query:   "CREATE PUBLICATION   ",
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			got, err := ExtractPulicationNameFromQuery(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExtractPulicationNameFromQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ExtractPulicationNameFromQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}
