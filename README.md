# KommatiPara Client Data Processor

## Overview

This application is designed for KommatiPara, a small company dealing with bitcoin trading. It processes and combines two separate datasets containing client information and financial details to create a targeted dataset for marketing purposes.

## Key Features

1. **Data Filtering**: Extracts client data specifically for the United Kingdom and the Netherlands.
2. **Data Privacy**: Removes personal identifiable information and sensitive financial data.
3. **Data Merging**: Combines client information with their financial details using a unique identifier.
4. **Column Renaming**: Improves readability by renaming specific columns for business users.
5. **Flexible Country Selection**: Allows processing for different countries through command-line arguments.

## How It Works

1. The application takes three command-line arguments:
   - Path to the client information dataset
   - Path to the financial details dataset
   - Countries to filter (e.g., ["United Kingdom","Netherlands"]

2. It uses PySpark to efficiently process the large datasets:
   - Filters clients based on specified countries
   - Removes sensitive information (PII and credit card numbers)
   - Joins the datasets on the 'id' field
   - Renames specified columns for clarity

3. The processed data is saved in the 'client_data' directory within the project root.



## Technical Details

- Developed using Python 3.8 and PySpark
- Implements logging for better traceability
- Utilizes generic functions for data filtering and column renaming
- Follows best practices for code organization and version control

## Testing

This project uses the `chispa` package for Spark-specific tests. To run the tests:

```
pytest tests/
```

## Distribution

The application can be packaged into a source distribution file. To create the package:

## Continuous Integration

This project uses GitHub Actions for continuous integration. The pipeline automatically runs tests, checks code style, and builds the distribution package on each push to the main branch and on pull requests.

## Requirements

All project dependencies are listed in the `requirements.txt` file. To install them:

```
pip install -r requirements.txt
```
