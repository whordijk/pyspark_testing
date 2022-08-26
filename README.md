# PySpark testing

Testing PySpark code using a Docker container where Java, Python and PySpark are preinstalled.

1. Build the Docker container image:

   ```bash
   docker build -t pyspark-test .
   ```

2. Run `main.py`:

   ```bash
   docker run -it -v ${PWD}:/code pyspark-test ./code/run.sh
   ```

   This will fire up the container, install additional dependencies required for running the main program, and then run it.


3. Test `main.py`:

   ```bash
   docker run -it -v ${PWD}:/code pyspark-test ./code/test.sh
   ```
   This will fire up the container, install additional dependencies required for testing the main program, and then test it.

