let config =
  /usr/local/var/dhall-kubernetes/api/Service/default
  //
  { name = "service-manager"
  , containerPort = 9000
  , outPort = 9000
  }

in /usr/local/var/dhall-kubernetes/api/Service/mkService config
