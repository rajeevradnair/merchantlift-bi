from merchantlift.spark import create_spark_session


def main() -> None:

    ss = create_spark_session("Smoke Test")
    ss.range(10).show()
    ss.stop()



if __name__ == "__main__":
    main()