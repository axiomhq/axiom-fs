module github.com/axiomhq/axiom-fs

go 1.25.3

require (
	github.com/BurntSushi/toml v1.6.0
	github.com/go-git/go-billy/v5 v5.7.0
	github.com/peterbourgon/ff/v3 v3.4.0
	github.com/willscott/go-nfs v0.0.3
	golang.org/x/sync v0.19.0
)

require (
	github.com/google/uuid v1.6.0 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7 // indirect
	github.com/rasky/go-xdr v0.0.0-20170124162913-1a41d1a06c93 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	github.com/willscott/go-nfs-client v0.0.0-20240104095149-b44639837b00 // indirect
	golang.org/x/sys v0.39.0 // indirect
)

replace github.com/willscott/go-nfs => github.com/tsenart/go-nfs v0.0.4-0.20260115144807-ef5168416b30
