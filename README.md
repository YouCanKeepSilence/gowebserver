# gowebserver
Repo just for test golang in webserver development with mongodb + kafka
May be later add docker here

To launch you need to clone this repo in subdirectory of your *$GOPATH/src*, run follow: 

```
$ dep ensure
$ go run main.go
```

Obviously you have to install dep package. 

```
$ brew install dep
$ brew upgrade dep
```

May be deploy is not right and you'll must to add some packages via `$ go get -u ...` (so i'll add this later)
