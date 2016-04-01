# we need the DLog URI to be set
if [[ -z "${DISTRIBUTEDLOG_URI}" ]]; then
  echo "Environment variable DISTRIBUTEDLOG_URI is not set."
  exit 1
fi

# add the jars from current dir to the class path (should be distributedlog-service)
for i in ./*.jar; do
  CLASSPATH="$i:${CLASSPATH}"
done

# add all the jar from lib/ to the class path
for i in ./lib/*.jar; do
  CLASSPATH="$i:${CLASSPATH}"
done
