# 🧠 Parallel Computing – Comparative Performance Analysis Using Apache Spark

![Comp. Paralela](https://github.com/brenomchd/Parallel-Computing/blob/master/Comp.%20Paralela.svg)

This project was developed with the goal of applying concepts learned in the **Parallel Computing** course to a real **Big Data** scenario, using the U.S. traffic accidents dataset and the **Apache Spark** framework.

---

## Summary

- [Project Overview](#project-overview)
- [Objective](#objective)
- [Repository Structure](#repository-structure)
- [Dataset Used](#dataset-used)
- [Technologies](#technologies)
- [Analyses Performed](#analyses-performed)
- [Results and Conclusions](#results-and-conclusions)
- [Interactive Dashboard](#interactive-dashboard)
- [How to Use This Project](#how-to-use-this-project)
- [Authors](#authors)
- [License](#license)


## 📌 Objective

To evaluate the efficiency of distributed parallel processing with Spark by comparing different levels of parallelism, data sizes, and types of analysis. An **interactive Power BI dashboard** was also developed to visualize results and compare sequential and parallel executions.

---

## 🗂️ Repository Structure

| File                        | Description                                                                 |
|----------------------------|-----------------------------------------------------------------------------|
| `mainParallel.py`          | Analysis script using Apache Spark (parallel)                              |
| `mainSequential.ipynb`     | Notebook with sequential analysis                                          |
| `Parallel_Results.csv`     | Results from parallel execution                                             |
| `Sequential_Results.csv`   | Results from sequential execution                                           |
| `Resultados.xlsx`          | Spreadsheet organizing data for cross-analysis                             |
| `Dashboard.pbix`           | Power BI dashboard with comparative visualizations                         |

---

## 📊 Dataset Used

- **Source:** Kaggle – [US Accidents (March 2023)](https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents)
- Over 7 million accident records in the U.S. (2016–2023)
- Attributes include location, weather, severity, and road conditions

---

## ⚙️ Technologies

- [Apache Spark (PySpark)](https://spark.apache.org/)
- Python 3 (Pandas, Matplotlib)
- Power BI
- Amazon EMR + S3 (for cluster execution)
- Jupyter Notebook

---

## 📈 Analyses Performed

- Accidents by state and time of day
- Relationship between weather and severity
- Frequency of intersections and traffic signals
- Extraction of accident types using regex
- Execution time, speedup, and scalability evaluation

---

## 🧪 Results and Conclusions

- **Significant speedup** in parallel executions compared to sequential ones
- **Best performance achieved with 4 to 8 executors**, depending on data fraction
- Increased overhead with too many threads for small datasets
- Visualizations demonstrate the **efficiency and scalability** of parallel computing

---

## 📊 Interactive Dashboard

The `Dashboard.pbix` (Power BI) file includes:

- Visual comparison between sequential and parallel executions
- Heatmaps, line charts, bar charts, and radar charts
- Analysis by dataset fraction and number of executors

---

## 🚀 How to Use This Project

### 1. Download the Dataset

To run this project locally, first download the dataset from Kaggle:

- Go to the dataset page: [US Accidents (Kaggle)](https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents)
- Click on **Download All**
- Extract the file (usually named `US_Accidents_Dec23_updated.csv`) into the **same directory** as the Python scripts or notebooks in this repository

> 📌 Make sure the dataset file is in the same folder as `mainParallel.py` or `mainSequential.ipynb`

### 2. Open and Run the Dashboard

You can also explore the results via Power BI:

- Download the `Dashboard.pbix` file from this repository
- Open it using [Power BI Desktop](https://powerbi.microsoft.com/en-us/desktop/)
- You can refresh the visuals or explore filters and interactions as needed

---

> ✅ All analysis files and the dashboard are ready to use once the dataset is correctly placed in the working directory
> 🗣️ The dashboard interface and labels are written in Brazilian Portuguese, as it was created for academic use in Brazil.


## 👨‍💻 Authors

Breno Machado Barros, Vinícius de Freitas Castro, Giordanna Santos e Souza, Gabriel Ferreira Silva, and Lauane Mateus Oliveira de Moraes.

– Project developed for the **Parallel Computing** course – UFG  

*Forked from the original repository by [gabrielsfg](https://github.com/gabrielsfg/Parallel-Computing)*

---

## 📝 License

This project is intended for educational purposes only. The use of the dataset follows the terms of the [Kaggle Dataset License](https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents/license).
