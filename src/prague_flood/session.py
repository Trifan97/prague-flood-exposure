"""Build a Spark session with Apache Sedona's spatial SQL functions registered.

On Wherobots the runtime hands you a ready session. Locally, Spark runs inside one JVM on
this machine, so the session has to be given the Sedona jars and told where to keep its
files.
"""

import os
import sys
from pathlib import Path

import pyspark
from pyspark.sql import SparkSession
from sedona.spark import SedonaContext

from prague_flood import config
from prague_flood.download import fetch


def local_session(
    master: str = "local[*]",
    app_name: str = "prague-flood",
    driver_memory: str = "3g",
    shuffle_partitions: int = 16,
) -> SparkSession:
    """Start, or reuse, a local Spark session with Sedona registered.

    ``master`` sets the number of cores: ``local[*]`` uses all of them, ``local[2]`` two.
    ``driver_memory`` is the JVM heap. In local mode the driver also does the executors'
    work, and Spark's 1 GB default makes cached data spill to disk.
    ``shuffle_partitions`` is how many tasks every groupBy, join or window shuffle
    creates. Spark's default of 200 is sized for clusters; on one machine it means
    hundreds of near-empty tasks per shuffle.
    """
    if "JAVA_HOME" not in os.environ:
        raise RuntimeError(
            "JAVA_HOME is not set. From the repository root, run: . .\\scripts\\dev_env.ps1"
        )
    # Spark starts Python worker processes with the first `python` on PATH, which can be a
    # different Python version from this one. Pin the workers to this interpreter.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    # Use the Spark that ships inside the pyspark package. Without this, pyspark's Windows
    # launcher has to guess the location, and warns when the path contains spaces.
    os.environ["SPARK_HOME"] = str(Path(pyspark.__file__).parent)

    # Put the Sedona jars straight onto the JVM classpath. The usual route,
    # spark.jars.packages, makes Spark copy each jar and set its file permissions, which
    # on Windows needs Hadoop's winutils.exe; Apache does not distribute it. In local mode
    # the executors run inside the driver's JVM, so the driver classpath covers them too.
    jars = [fetch(source, config.JARS_DIR) for source in config.SEDONA_JARS]
    classpath = os.pathsep.join(str(jar) for jar in jars)

    builder = (
        SedonaContext.builder()
        .master(master)
        .appName(app_name)
        .config("spark.driver.extraClassPath", classpath)
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.local.dir", str(config.SPARK_CACHE_DIR / "tmp"))
        # Fixed time zone, so timestamps do not depend on the machine running the code.
        .config("spark.sql.session.timeZone", "UTC")
        # The console progress bars garble pytest and log output.
        .config("spark.ui.showConsoleProgress", "false")
    )
    # SedonaContext is untyped, so state the type of what it returns.
    session: SparkSession = SedonaContext.create(builder.getOrCreate())
    return session
