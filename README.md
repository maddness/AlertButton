# Тревожный браслет

Исследование датасета и описание алгоритма - *dataset_investigation.ipynb*

Запуск Spark скрипта:

```sh 
spark-submit --master local --py-files objects.py spark_job.py <input hdfs file> <output hdfs file>
```

Исходный файл - *alert_button.csv*, при запуске скрипта на другом датасете просьба удалить первую строку - названия колонок.

Результат работы приложен в файле *result.csv*
