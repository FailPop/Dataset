from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean
from pyspark.sql.types import DoubleType
import pyspark.sql.functions as F


def read_csv_file(spark, input_file):
    df = spark.read.csv(input_file, header=True, inferSchema=True)
    return df


def find_min_max(df, column_name):
    select_df = df.select(column_name)
    max_value = select_df.agg({column_name: "max"}).collect()[0][0]
    min_value = select_df.agg({column_name: "min"}).collect()[0][0]
    print("Max: ", max_value, "\nMin: ", min_value)


def sort_column(df, column_name, ascending=True):
    sorted_df = df.orderBy(column_name, ascending=ascending)
    sorted_df.show(20)


def find_mean(df, column_name):
    mean_value = df.select(mean(col(column_name))).collect()[0][0]
    print("Mean: ", mean_value)


def calculate_percentile(df, column_name):
    quantiles = (df.agg(F.expr('percentile_approx(' + column_name + ', array(0.25, 0.5, 0.75))')).collect()[0][0])
    print("25% Quantile: ", quantiles[0], "\n50% Quantile: ", quantiles[1], "\n75% Quantile: ", quantiles[2])


def find_median(df, column_name):
    quantiles = (df.agg(F.expr('percentile_approx(' + column_name + ', array(0.5))')).collect()[0][0])
    print("Median: ", quantiles[0])


def count_rows(df):
    count = df.count()
    print("Количество строк в файле: ", count)


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
    spark = SparkSession.builder \
        .appName("SparkSession") \
        .config("spark.executor.cores", "4") \
        .config("spark.default.parallelism", "8") \
        .config("spark.executor.memory", "12g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    input_file = "C:/Users/vasky/Desktop/hacaton/dataset.csv"
    print("Чтение файла csv")
    df = read_csv_file(spark, input_file)
    row_name = df.columns
    while True:
        print("\nНаименование колонок в файле", "\n", row_name)
        print("\nМеню:")
        print("1. Вывести 20 строк dataset.csv")
        print("2. Найти min и max в выбранной колонке")
        print("3. Сортировать колонку")
        print("4. Найти mean в выбранной колонке")
        print("5. Найти количество строк")
        print("6. Найти percentile в выбранной колонке (25%, 50%, 75%)")
        print("7. Найти median в выбранной колонке")
        print("8. Фильтрация")
        print(". Выйти")

        choice = input("Выберите опцию: ")

        if choice == "1":
            df.show(20)
        elif choice == "2":
            column_name = input("Введите имя колонки: ")
            find_min_max(df, column_name)
        elif choice == "3":
            column_name = input("Введите имя колонки: ")
            ascending = input("Сортировать по возрастанию (да/нет): ").lower() == "да"
            sort_column(df, column_name, ascending)
        elif choice == "4":
            column_name = input("Введите имя колонки: ")
            find_mean(df, column_name)
        elif choice == "5":
            count_rows(df)
        elif choice == "6":
            column_name = input("Введите имя колонки: ")
            calculate_percentile(df, column_name)
        elif choice == "7":
            column_name = input("Введите имя колонки: ")
            find_median(df, column_name)
        elif choice == "8":
            column_name = input("Введите имя колонки: ")
            condition = input("Выберите условие фильтрации (больше/меньше/равно): ").lower()
            value = input("Введите значение для фильтрации: ")
            filter_data(df, column_name, condition, value)
        elif choice == ".":
            confirm_exit = input("Вы уверены, что хотите выйти? (да/нет): ").lower()
            if confirm_exit == "да":
                print("Выход из программы.")
                break
        else:
            print("Некорректный выбор. Пожалуйста, выберите опцию из меню.")

    spark.stop()


if __name__ == "__main__":
    main()
