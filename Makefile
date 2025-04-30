CONFIG_PATH := ${HOME}/.prolog/

.PHONY: init
init:
	mkdir -p ${CONFIG_PATH}
	cp -f utils/model.conf ${CONFIG_PATH}/model.conf
	cp -f utils/policy.csv ${CONFIG_PATH}/policy.csv

.PHONY: gencert
gencert:
	cfssl gencert \
		-initca utils/ca-csr.json | cfssljson -bare ca

	cfssl gencert \
		-ca ca.pem \
		-ca-key ca-key.pem \
		-config utils/ca-config.json \
		-profile server \
		utils/server-csr.json | cfssljson -bare server

	cfssl gencert \
		-ca ca.pem \
		-ca-key=ca-key.pem \
		-config utils/ca-config.json \
		-profile client \
		-cn="root" \
		utils/client-csr.json | cfssljson -bare root-client

	cfssl gencert \
		-ca ca.pem \
		-ca-key=ca-key.pem \
		-config utils/ca-config.json \
		-profile client \
		-cn="nobody" \
		utils/client-csr.json | cfssljson -bare nobody-client

	mv *.pem *.csr ${CONFIG_PATH}

.PHONY: test
test: init
	go test -race ./...

.PHONY: compile
compile:
	protoc api/v1/*.proto \
		--go_out=. \
		--go-grpc_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		--proto_path=.
