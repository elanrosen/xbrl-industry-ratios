import requests
import pandas as pd


def filter_for_correct_values_10K(concepts):
    # Step 1: Remove all rows with concept.local-name equal to "NetIncomeLoss"
    filtered_concepts = concepts[concepts['concept.local-name'] != 'NetIncomeLoss']

    # Reset the index
    filtered_concepts.reset_index(drop=True, inplace=True)


    return filtered_concepts

def filter_for_correct_values_10Q(concepts):
    # Step 1: Remove all rows with concept.local-name equal to "NetIncomeLoss"
    filtered_concepts = concepts[concepts['concept.local-name'] != 'NetIncomeLoss']

    # Reset the index
    filtered_concepts.reset_index(drop=True, inplace=True)

    # Filter out rows based on specified conditions
    filtered_concepts = filtered_concepts[
        ~(
            (
                (filtered_concepts['concept.local-name'] == 'AssetsCurrent') |
                (filtered_concepts['concept.local-name'] == 'InventoryNet') |
                (filtered_concepts['concept.local-name'] == 'PropertyPlantAndEquipmentNet') |
                (filtered_concepts['concept.local-name'] == 'StockholdersEquity') |
                (filtered_concepts['concept.local-name'] == 'LongTermDebtCurrent') |
                (filtered_concepts['concept.local-name'] == 'RevenueFromContractWithCustomerExcludingAssessedTax') |
                (filtered_concepts['concept.local-name'] == 'Revenue') |
                (filtered_concepts['concept.local-name'] == 'Revenues') |
                (filtered_concepts['concept.local-name'] == 'NetSales') |
                (filtered_concepts['concept.local-name'] == 'ResearchAndDevelopmentExpense') |
                (filtered_concepts['concept.local-name'] == 'OperatingIncomeLoss') |
                (filtered_concepts['concept.local-name'] == 'NetIncomeLoss') |
                (filtered_concepts['concept.local-name'] == 'ProfitLoss') |
                (filtered_concepts['concept.local-name'] == 'Assets') |
                (filtered_concepts['concept.local-name'] == 'Liabilities') |
                (filtered_concepts['concept.local-name'] == 'SalesRevenueNet')
            ) &
            (filtered_concepts['member.is-base'].isin([True, False]))
        )
    ]

    # Convert 'period.year' to integer for comparison
    filtered_concepts['period.year'] = filtered_concepts['period.year'].astype(int)

    # Selecting rows where 'concept.local-name' is one of the specified values
    name_condition = filtered_concepts['concept.local-name'].isin(['InventoryNet', 'AssetsCurrent', 'PropertyPlantAndEquipmentNet', 'StockholdersEquity', "Assets", 'Liabilities', 'ProfitLoss', 'NetIncomeLoss', 'SalesRevenueNet', 'Revenues', 'ResearchAndDevelopmentExpense'])

    # Selecting rows where 'period.fiscal-period' contains 'Q'
    period_condition = filtered_concepts['period.fiscal-period'].str.contains('Q')

    # Selecting rows where 'period.year' is equal to the maximum period year
    max_year_condition = filtered_concepts['period.year'] == filtered_concepts['period.year'].max()

    # Combining all conditions using logical AND operation
    combined_condition = name_condition & period_condition & max_year_condition

    # Filtering rows that meet all conditions
    filtered_concepts = filtered_concepts[combined_condition]

    return filtered_concepts


def find_concepts(access_token, report_id, report_document_type):
  """
  Function: find_concepts

  Description:
      Retrieves specific financial concepts from a given quarterly report and returns their values.

  Parameters:
      access_token (str): Access token for authentication.
      report_id (str): ID of the report from which concepts are to be retrieved.

  Returns:
      dict or None: A dictionary containing financial concepts and their corresponding values.
      Returns None if the concepts could not be retrieved or if there are no relevant concepts.

      dict has these keys 'AssetsCurrent', 'InventoryNet', 'PropertyPlantAndEquipmentNet', 'StockholdersEquity',
                            'LongTermDebtCurrent', 'RevenueFromContractWithCustomerExcludingAssessedTax',
                            'Revenue', 'Revenues', 'NetSales', 'ResearchAndDevelopmentExpense',
                            'OperatingIncomeLoss', 'NetIncomeLoss'

  """
  fact_search_url = 'https://api.xbrl.us/api/v1/fact/search'

  params = {
      'report.id': report_id,
      'fields': ','.join([
          'concept.local-name.sort(ASC)',
          'entity.cik',
          'report.sic-code',
          'period.year',
          'period.end',
          'period.instant',
          'period.fiscal-period',
          'fact.value',
          # 'footnote.role',
          'dimensions',
          'dimensions.count',
          # 'dts.target-namespace',
          # 'dts.id',
          'fact.value-link',
          'member.is-base',
          'member.local-name',
          'member.member-value',
          'member.namespace'
          'fact.accuracy-index'
      ])
  }

  response = requests.get(fact_search_url, params=params, headers={'Authorization': 'Bearer {}'.format(access_token)})

  if response.status_code == 200:
      facts_data = response.json()
      df = pd.DataFrame(facts_data['data'])
      required_concepts = ['AssetsCurrent', 'StockholdersEquity',
                            'LongTermDebtCurrent',
                            "Assets", 'Liabilities']
      ## Some companies ($PSX) report Total Revnues and Other Income (like equity earnings of affiliates) there wasn't a gap figure for this so excluded (didn't seem meaninufl)

      filtered_df = df[df['concept.local-name'].isin(required_concepts)]
      if report_document_type == '10-Q':
        filtered_df = filter_for_correct_values_10Q(filtered_df)
      else:
        filtered_df = filter_for_correct_values_10K(filtered_df)
      concepts_dict = dict(zip(filtered_df['concept.local-name'], filtered_df['fact.value']))

      return concepts_dict
  elif response.status_code == 429:
      print("API rate limit exceeded. Terminating the program.")
      sys.exit(1)  # Exit the program with an error code
  else:
      print("Error:", response.status_code, response.text)
      return None
