# Isolation Test

Test and organize transaction isolation levels.

https://isolation-test-90557.web.app

## test on local

### setup

load env vars for go_ibm_db

```console
pushd $(go env GOPATH)/pkg/mod/github.com/ibmdb/go_ibm_db\@v0.4.3/installer
source setenv.sh
popd
```

start containers

```console
docker compose up -d
```

setup database for db2

```console
docker compose exec db2 bash -c "su - db2inst1"

[db2inst1@1fe391891712 ~]$ db2 "CREATE DATABASE TESTDB2" # take a while...
DB20000I  The CREATE DATABASE command completed successfully.

[db2inst1@1fe391891712 ~]$ db2 "UPDATE DATABASE CONFIGURATION for TESTDB2 USING cur_commit DISABLED"
DB20000I  The UPDATE DATABASE CONFIGURATION command completed successfully.
```

### run

```console
go test ./test/ -v
```

## web ui

### local

```console
cd web
npm install
npm run dev
```

### deploy

(require firebase hosting setup)

```console
cd web
npm run build
npm run deploy
```
