#!/bin/bash
echo '
use admin
db.auth("root", "example")
use global
db.createUser(
{
  user: "test-user",
  pwd: "test-password",
  roles: [ { role: "readWrite", db: "global" } ]
})' | mongo
