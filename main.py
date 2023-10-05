import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean
from pyspark.sql.types import DoubleType
import pyspark.sql.functions as F

cache_files_to_clean = []


def create_spark_session():
    spark = SparkSession.builder \
        .appName("Dataframe") \
        .config("spark.executor.memory", "8g") \
        .config("spark.executor.cores", "4") \
        .config("spark.executor.instances", "2") \
        .config("spark.memory.offHeap.enabled", "true") \
        .config("spark.memory.offHeap.size", "2g") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.dynamicAllocation.executorIdleTimeout", "30s") \
        .getOrCreate()
    return spark


def read_csv_file(spark, input_file):
    df = spark.read.csv(input_file, header=True, inferSchema=True)
    return df


def find_min_max(df, column_name):
    cache_file = f"{column_name}_min_max_cache.txt"
    cache_files_to_clean.append(cache_file)
    if os.path.isfile(cache_file):
        with open(cache_file, "r") as file:
            max_value, min_value = map(float, file.read().split())
        print("Max: ", max_value, "\nMin: ", min_value)
    else:
        select_df = df.select(column_name)
        max_value = select_df.agg({column_name: "max"}).collect()[0][0]
        min_value = select_df.agg({column_name: "min"}).collect()[0][0]
        print("Max: ", max_value, "\nMin: ", min_value)
        with open(cache_file, "w") as file:
            file.write(f"{max_value} {min_value}")


def sort_column(df, column_name, ascending=True):
    if ascending:
        cache_file = f"{column_name}_sorted_asc_cache.txt"
    else:
        cache_file = f"{column_name}_sorted_desc_cache.txt"
    cache_files_to_clean.append(cache_file)
    if os.path.isfile(cache_file):
        with open(cache_file, "r") as file:
            sorted_data = file.read()
        print(sorted_data)
    else:
        sorted_df = df.orderBy(column_name, ascending=ascending)
        sorted_df.show(20)
        with open(cache_file, "w") as file:
            file.write(sorted_df._jdf.showString(20, 20, False))


def find_mean(df, column_name):
    cache_file = f"{column_name}_mean_cache.txt"
    cache_files_to_clean.append(cache_file)
    if os.path.isfile(cache_file):
        with open(cache_file, "r") as file:
            mean_value = float(file.read())
        print("Mean: ", mean_value)
    else:
        mean_value = df.select(mean(col(column_name))).collect()[0][0]
        print("Mean: ", mean_value)
        with open(cache_file, "w") as file:
            file.write(str(mean_value))


def calculate_percentile(df, column_name):
    cache_file = f"{column_name}_percentile_cache.txt"
    cache_files_to_clean.append(cache_file)
    if os.path.isfile(cache_file):
        with open(cache_file, "r") as file:
            quantiles = list(map(float, file.read().split()))
        print("25% Quantile: ", quantiles[0], "\n50% Quantile: ", quantiles[1], "\n75% Quantile: ", quantiles[2])
    else:
        quantiles = (df.agg(F.expr('percentile_approx(' + column_name + ', array(0.25, 0.5, 0.75))')).collect()[0][0])
        print("25% Quantile: ", quantiles[0], "\n50% Quantile: ", quantiles[1], "\n75% Quantile: ", quantiles[2])
        with open(cache_file, "w") as file:
            file.write(" ".join(map(str, quantiles)))


def find_median(df, column_name):
    cache_file = f"{column_name}_median_cache.txt"
    cache_files_to_clean.append(cache_file)
    if os.path.isfile(cache_file):
        with open(cache_file, "r") as file:
            median_value = float(file.read())
        print("Median: ", median_value)
    else:
        quantiles = (df.agg(F.expr('percentile_approx(' + column_name + ', array(0.5))')).collect()[0][0])
        median_value = quantiles[0]
        print("Median: ", median_value)
        with open(cache_file, "w") as file:
            file.write(str(median_value))


def count_rows(df):
    cache_file = "count_rows_cache.txt"
    cache_files_to_clean.append(cache_file)
    if os.path.isfile(cache_file):
        with open(cache_file, "r") as file:
            count = int(file.read())
        print("Количество строк в файле: ", count)
    else:
        count = df.count()
        print("Количество строк в файле: ", count)
        with open(cache_file, "w") as file:
            file.write(str(count))


def is_numeric_column(df, column_name):
    return isinstance(df.schema[column_name].dataType, DoubleType)


def filter_data(df, column_name, condition, value):
    filtered_df = df
    if is_numeric_column(df, column_name):
        if condition.lower() == "больше":
            filtered_df = filtered_df.filter(col(column_name) > float(value))
        elif condition.lower() == "меньше":
            filtered_df = filtered_df.filter(col(column_name) < float(value))
        elif condition.lower() == "равно":
            filtered_df = filtered_df.filter(col(column_name) == float(value))
        else:
            print("Некорректное условие фильтрации.")
            return
        filtered_df.show(20)
    else:
        print("Колонка не является числовой.")


def main():
    spark = create_spark_session()
    input_file = "C:/Users/vasky/Desktop/hacaton/dataset.csv"
    print("Чтение файла csv")
    df = read_csv_file(spark, input_file)
    row_name = df.columns
    try:
        while True:
            print("\nНаименование колонок в файле", "\n", row_name)
            print("\nМеню:")
            print("1. Вывести 20 строк dataset.csv")
            print("2. Найти min и max в выбранной колонке")
            print("3. Сортировать колонку")
            print("4. Найти mean в выбранной колонке")
            print("5. Найти количество строк")
            print("6. Найти percentile (25%, 50%, 75%)")
            print("7. Найти median")
            print("8. Фильтрация")
            print("9. Выйти")

            choice = input("Выберите опцию: ")
            if int(choice) not in range(1, 9):
                print("Вы выбрали неверное опцию")
                continue

            if int(choice) != 1:
                column_name = input("Введите имя колонки: ")
                if column_name not in row_name:
                    print("Вы выбрали несуществующую колонку")
                    continue

            if choice == "1":
                df.show()
            elif choice == "2":
                find_min_max(df, column_name)
            elif choice == "3":
                ascending = input("Сортировать по возрастанию (да/нет): ").lower() == "да"
                sort_column(df, column_name, ascending)
            elif choice == "4":
                find_mean(df, column_name)
            elif choice == "5":
                count_rows(df)
            elif choice == "6":
                calculate_percentile(df, column_name)
            elif choice == "7":
                find_median(df, column_name)
            elif choice == "8":
                condition = input("Выберите условие фильтрации (больше/меньше/равно): ").lower()
                value = input("Введите значение для фильтрации: ")
                filter_data(df, column_name, condition, value)
            elif choice == "9":
                confirm_exit = input("Вы уверены, что хотите выйти? (да/нет): ").lower()
                if confirm_exit == "да":
                    print("Выход из программы.")
                    break

    except KeyboardInterrupt:
        print("Процесс завершен по запросу пользователя.")
    finally:
        for cache_file in cache_files_to_clean:
            if os.path.isfile(cache_file):
                os.remove(cache_file)

        spark.stop()


if __name__ == "__main__":
    main()
