let config =  /usr/local/var/dhall-kubernetes/api/Deployment/default
  ⫽ { name = "catalog-manager"
  , hostAliases = /env/hostaliases
  , configMapVolumes = ./configMapVolumes
  , pathVolumes = ./pathVolumes
  , containers =
                [   /usr/local/var/dhall-kubernetes/api/Deployment/defaultContainer
                  ⫽ /env/nexus
                  ⫽ { name = "catalog-manager"
                    , imageName = "daf-catalog-manager"
                    , imageTag = "2.0.19-SNAPSHOT"
                    , port = [ 9000 ] : Optional Natural
                    , mounts = ./volumeMounts
                    , simpleEnvVars = [ { mapKey = "JAVA_OPTS", mapValue = "-server -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:+PerfDisableSharedMem -XX:+ParallelRefProcEnabled -Xmx2g -Xms2g -XX:MaxPermSize=1024m" }
                    , { mapKey = "KRB5_CONFIG", mapValue = "/etc/extKerberosConfig/krb5.conf" } ]
                    , secretEnvVars = ./secretEnvVars
                    }
                ]
            }

in   /usr/local/var/dhall-kubernetes/api/Deployment/mkDeployment config
