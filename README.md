# Data Agency
Agentic data retrival and analysis. 

## Why we need another agent?
1. Serious data analysis requires transparency, not black-box results.
2. Context isn’t good for storing data; better to pass dataframes explicitly and generate code step by step.

## Data retrival
Assuming specialized module to download data. Avaiable upon request. 

* **Coverage**: 1500 series of cross-country panel data, for macrofinance, and international finance. 

* **Format**: A long format panel data with unified country codes.

## Using Data
We can access from ipython magic command. See `sample/full_worksflow.ipynb`.

Available commands:
- `$data find <command>`: Find data based on the provided criteria.
- `$data load <command>`: Load data into the environment.
- `$data describe <dataframes>`: Get a description of the provided dataframes.
- `$data analyze <command>`: Generate the code for analyzing the loaded data.
- `$data config <command>`: Configure data settings.

For more information on each command, use following command in jupyter.
```
%load_ext data_agency
# $data <command> help
```


## Installation
```bash
pip install data-agency@git+https://github.com/yoki/data-agency.git@0.x.y
```

Other than pip, docker container and LLM (Gemini) API setup is needed.

### Docker
Docker must be installed. Tested with docker installed in WSL, not in docker desptop. 

To use in devcontainer, you should install official docker, not repository one, and use fuse-overlayfs (or overlay2). Default vfs is slow (as of Aug 2025). 
```dockerfile
RUN curl -fsSL https://get.docker.com -o get-docker.sh && sh get-docker.sh && rm get-docker.sh
RUN mkdir -p /etc/docker && \
    echo '{\n  "storage-driver": "fuse-overlayfs"\n}' > /etc/docker/daemon.json
```

### API key and other env vars
Create `.env` file:
```bash
GEMINI_API_KEY_FOR_DATA_AGENCY=your_gemini_api_key_here
```

**File locations (priority order):**
1. `$DATA_AGENCY_DOTENV_PATH` (if set)
2. `./env` (current directory)
3. `/secrets/data_agency/.env` (Docker/devcontainer)
4. `~/.config/codegen-agent/.env` (Linux/WSL)
5. `%LOCALAPPDATA%\data_agency\data_agency\.env` (Windows)

```json
    "mounts": [
        "type=bind,source=/mnt/c/my-path-to-secret,target=/secrets/data_agency,readonly",
    ],
```

## Running in devcontainer?
Check `.devcontainer` folder of this repository.



## Sample output
`
$ python /workspaces/data-agency/sample/code_generation.py
`

**Execution Results (Attempt 1):**
```
Average Tip Percentage by Day of Week and Time:
time     Dinner      Lunch
day                       
Thur  15.974441  16.130074
Fri   15.891611  18.876489
Sat   15.315172        NaN
Sun   16.689729        NaN

```


```python
**Code Assessment:** The generated code meets the requirements.

# User request: Calculate average tip percentage by day of week and time (lunch/dinner), create a pivot table, and visualize with a heatmap

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Calculate tip percentage
tips_data['tip_percentage'] = (tips_data['tip'] / tips_data['total_bill']) * 100

# Define the order of days for better visualization
day_order = ['Thur', 'Fri', 'Sat', 'Sun']
tips_data['day'] = pd.Categorical(tips_data['day'], categories=day_order, ordered=True)

# Create a pivot table for average tip percentage by day and time
pivot_table = tips_data.pivot_table(values='tip_percentage', index='day', columns='time', aggfunc='mean')

# Display the pivot table
print("Average Tip Percentage by Day of Week and Time:")
print(pivot_table)

# Visualize with a heatmap
plt.figure(figsize=(8, 6))
sns.heatmap(pivot_table, annot=True, fmt=".2f", cmap="YlGnBu", linewidths=.5)
plt.title('Average Tip Percentage by Day and Time')
plt.ylabel('Day of Week')
plt.xlabel('Time of Day')
plt.show()
```