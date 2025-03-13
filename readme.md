# **Yellow Canary Data Tech Test**  

## **How to Run the Code**  

### **1. Clone the Repository and Navigate to the Directory**  
```bash
git clone https://github.com/craigli-20240902/YellowCanaryDataTechTest.git && cd YellowCanaryDataTechTest
```

### **2. Set Up a Virtual Environment**  

Python version used for this project: **3.12.2**

You can use pyenv to install and manage multiple versions of Python runtime.


#### **Create the Virtual Environment**  

```bash
python -m venv .venv
```

#### **Activate the Virtual Environment**  
- **On macOS/Linux:**  
  ```bash
  source .venv/bin/activate
  ```
- **On Windows (PowerShell):**  
  ```powershell
  .venv\Scripts\Activate
  ```

### **3. Install Dependencies**  
```bash
pip install -r requirements.txt
```

### **4. Run the Pipeline**  
Execute the script and follow the prompts to enter:  
- **Base Directory:** The absolute path of the `YellowCanaryDataTechTest` directory on your machine.  
- **Excel File Name:** The name of the Excel file located in `data/raw/` (e.g., `"Sample Super Data.xlsx"`).  

```bash
python pipeline/pipeline.py
```

### **5. Output Files**  
- Extracted CSV files (`Disbursements.csv`, `Paycodes.csv`, and `Payslips.csv`) will be saved in:  
  ```
  data/extracted/
  ```
- The final **metrics report** (`metrics.csv` and `metrics.xlsx`) will be saved in:  
  ```
  metrics/
  ```
---

## **Running Tests and Generating Coverage Reports**  

### **6. Run Unit Tests**  
```bash
pytest pipeline/test_pipeline.py
pytest pipeline/test_pipeline_utils.py
```

### **7. Generate a Coverage Report**  
```bash
coverage run -m pytest
coverage report -m
```

### **8. Generate an HTML Coverage Report (Optional)**  
```bash
coverage html
```
This creates an **HTML report** in the `htmlcov/` directory.

### **9. Remove Coverage Files (Optional)**  
```bash
coverage erase && rm -rf htmlcov
```