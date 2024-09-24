"""
1. Find all Snowflake views used by datasources in "Curated Data Sources" and workbooks in "UAT" and "Production
2. Use the Object Dependency table in Snowflake to find what other Snowflake tables are used in those views
3. Output relationships to /exposures/exposures.yml
"""

import tableauserverclient as tsc
from pprint import pprint
import pandas as pd
import os
import yaml
from pathlib import Path
import snowflake.connector
from src.metadata_explorer.extras.auth import server, tableau_auth

data_source_folders = ['Developer Data Sources', 'Data Engineering Prototypes']
workbook_folders = ['Production', 'Internal Project Review (UAT)', 'Power User Prototypes', 'Support', 'Dev']


def main():
    """
    1. Find all Snowflake views used by datasources in "Curated Data Sources" and workbooks in "UAT" and "Production
    2. Use the Object Dependency table in Snowflake to find what other Snowflake tables are used in those views
    3. Output relationships to /exposures/exposures.yml

    :param None:
    :return None:
    """

    # Initiate server session
    with server.auth.sign_in(tableau_auth):

        # Change Pandas settings to show as much data as possible, no cutoffs
        change_pandas_settings(max_columns=None, max_rows=None, max_colwidth=1000, width=0)

        # Add child folders to data source and workbook folders list
        global data_source_folders, workbook_folders
        data_source_folders = add_child_projects_to_list(data_source_folders)
        workbook_folders = add_child_projects_to_list(workbook_folders)

        # Run metadata queries in Tableau, find dependencies between Tableau objects and Snowflake views
        datasource_and_workbooks_df = get_datasources_and_workbooks()
        datasource_and_workbooks_df = datasource_and_workbooks_df[[
            'database',
            'schema',
            'table_or_view',
            'projectName',
            'content_type',
            'name',
            'vizportalUrlId',
            'owner_name',
            'owner_email'
        ]].drop_duplicates(keep='first')
        datasource_and_workbooks_df.to_csv('to_csv.csv')

        # Pull down Snowflake dependencies here
        #snowflake_dependencies_df = query_snowflake_dependencies()

        # Join Tableau metadata to Snowflake dependencies
        #merged_dataframes = pd.merge(left=datasource_and_workbooks_df, right=snowflake_dependencies_df, how='inner',
        #                             left_on=['database', 'schema', 'table_or_view'],
        #                             right_on=['REFERENCING_DATABASE', 'REFERENCING_SCHEMA', 'REFERENCING_OBJECT_NAME'])

        # Reduce to just content name, type, and dependency table
        #grouped_dataframe = flatten_dependencies(merged_dataframes)

        # Create dependency file
        #create_output_file(grouped_dataframe)


def create_output_file(df):
    """
    Create exposures.yml if not exist
    Write to file from dataframe

    :param df:
    :return None:
    """

    print('Generating file contents')

    # Create empty file
    repo_directory = Path.cwd()
    exposures_dir = Path.joinpath(repo_directory, 'exposures')
    filename = os.path.join(exposures_dir, 'exposures.yml')
    print('Creating file: ', filename)
    with open(filename, 'w+') as new_file:
        print('Created file: ', filename)

    # Create file content
    global exposure_contents
    exposure_contents = {'version': 1, 'exposures': []}
    df.apply(populate_exposure_contents, axis=1)
    with open(filename, 'w') as file:
        yaml.dump(exposure_contents, file, sort_keys=False)
    print('Wrote content to file: ', filename)


def get_datasources_and_workbooks():
    """
    Pull datasource -> Snowflake view dependencies
    Pull workbook - > Snowflake view dependencies
    Combine into one dataframe
    :param None:
    :return None:
    """

    print("Pulling dependencies from Tableau")

    # Pull Curated Data Sources Snowflake Tables in JSON form
    global data_source_folders
    data_source_folders_str = '"' + '" , "'.join(data_source_folders) + '"'

    query_string = f"""
        query getSnowflakeTables{{
          publishedDatasources (filter:{{projectNameWithin:[{data_source_folders_str}]}}){{
            name
            vizportalUrlId
            upstreamTables {{
              fullName
            }}
            projectName
            owner {{
              name
              email
            }}
          }}
        }}"""
    curated_data_sources_result = query_metadata(query_string)

    print("curated_data_sources_result:", curated_data_sources_result)

    # Flatten Curated Data Sources Snowflake Tables to Dataframe
    curated_data_sources_df = flatten_results(curated_data_sources_result)

    # Pull Embedded Workbook Snowflake Tables in JSON form
    dev_uat_prod_projects = add_child_projects_to_list(workbook_folders)

    query_string = """query getSnowflakeTables2{
            workbooks (filter: {projectNameWithin:["%s"]}){
                name
                vizportalUrlId
                upstreamTables{
                    fullName
                    }
                projectName
                owner {
                  name
                  email
                }
            }
        }""" % '","'.join(dev_uat_prod_projects)
    dev_uat_prod_result = query_metadata(query_string)

    # Flatten Embedded Workbook Snowflake Tables to Dataframe
    dev_uat_prod_df = flatten_results(dev_uat_prod_result)

    # Union Tableau metadata dataframes here
    data_sources_and_workbooks_df = pd.concat([curated_data_sources_df, dev_uat_prod_df])

    return data_sources_and_workbooks_df


# Function to run and process metadata query
def query_metadata(query_text):
    """
    :param query_text:
    :return df:
    """

    result = server.metadata.query(query_text)

    # Display warnings/errors (if any)
    if result.get("errors"):
        print("### Errors/Warnings:")
        pprint(result["errors"])

    # Flatten results in pandas dataframe and print
    if result.get("data"):
        return result.get("data")


def flatten_results(result):
    """
    :param result:
    :return df:
    """
    # Pull only first data item
    keys = list(result.keys())
    first_key = keys[0]
    df = pd.DataFrame(result[first_key])

    # Turning upstreamTables into an explode-able column
    def get_table_names(value, axis):
        """
        Turn dictionary into list
        :param value:
        :return table_names:
        """

        table_names = []
        for elem in value:
            table_name = elem['fullName'].lower()
            table_name = table_name.replace('[', '').replace(']', '').replace('"', '')
            table_names.append(table_name)
        return table_names

    df['upstreamTables2'] = df['upstreamTables'].apply(func=get_table_names, axis=1)

    # Exploding the upstreamTables into multiple rows
    df = df.explode('upstreamTables2')

    # Creating owner_email column
    def get_owner_email(value, axis):
        return value.get('email')

    df['owner_email'] = df['owner'].apply(func=get_owner_email, axis=1)

    # Creating owner_name column
    def get_owner_name(value, axis):
        return value.get('name')

    df['owner_name'] = df['owner'].apply(func=get_owner_name, axis=1)

    # Splitting upstreamTables into db, schema, table, removing records with null database values
    df[['database', 'schema', 'table_or_view']] = df['upstreamTables2'].str.split('.', expand=True)
    df = df.drop(df[df.table_or_view.isnull()].index)

    # Describe content type
    df['content_type'] = first_key

    return df


def flatten_dependencies(df):
    """
    Grouping by Tableau content_type, name, vizportalUrlId, owner_name, owner_email,
    Create a comma-separated list of all Snowflake tables that feed into the Tableau object
    :param df:
    :return grouped_dataframe:
    """

    print("Combining Tableau and Snowflake data to create a comma-separated list of dependencies for each Tableau "
          "datasource or workbook")

    group_by_columns = ['content_type', 'name', 'vizportalUrlId', 'owner_name', 'owner_email']
    all_columns = group_by_columns + ['dependency_fullname']
    grouped_dataframe = df[all_columns]
    grouped_dataframe = grouped_dataframe.groupby(group_by_columns)[['dependency_fullname']] \
        .transform(lambda x: ','.join(x))
    grouped_dataframe = pd.concat([df[group_by_columns], grouped_dataframe], axis=1)
    grouped_dataframe = grouped_dataframe.drop_duplicates().reset_index()

    return grouped_dataframe


def add_child_projects_to_list(project_search_list):
    """
    Sales project, for example, contains many more projects containing workbooks.
    :param project_search_list:
    :return project_names_to_document:
    """

    project_ids_to_document = []
    project_names_to_document = []

    # Find all Dev, UAT, and Production projects
    all_projects, _ = server.projects.get()

    # Find production folder id
    for project in all_projects:
        if project.name in project_search_list:
            project_ids_to_document.append(project.id)

    # Populate children
    # Thought a hard-coded loop ceiling was safer and more succinct than having a 'while' condition, open to feedback
    for x in range(0, 5):
        for ptd in project_ids_to_document:
            for project in all_projects:
                if project.parent_id == ptd and project.id not in project_ids_to_document:
                    project_ids_to_document.append(project.id)

    # Find names by project id
    for p in all_projects:
        if p.id in project_ids_to_document:
            project_names_to_document.append(p.name)

    return project_names_to_document


def query_snowflake_dependencies():

    """
    Pull all Snowflake dependencies
    Add column that can be compared with Tableau dependencies dataframe
    :return df:
    """
    print('Pulling Snowflake dependencies')

    # Connect to Snowflake and run query text
    query_text = \
        'select * from prod_reporting.snowflake_account_usage.object_dependencies where referenced_database is not null'
    ctx = snowflake.connector.connect(user='janet.shen@sunstateequip.com',
                                      account='uxb15565',
                                      warehouse='medium_analyst_wh',
                                      role='analyst_role',
                                      authenticator='externalbrowser')
    cur = ctx.cursor()
    cur.execute(query_text)

    # Fetch the result set from the cursor and deliver it as the pandas DataFrame.
    df = cur.fetch_pandas_all()
    df = df.apply(lambda x: x.astype(str).str.lower())
    df['dependency_fullname'] = \
        df['REFERENCED_DATABASE'] + '.' + \
        df['REFERENCED_SCHEMA'] + '.' + \
        df['REFERENCED_OBJECT_NAME']

    cur.close()

    return df


def populate_exposure_contents(row):

    """
    Used via apply() to each record in a dataframe, appends the parsed row to global variable
    :param row:
    """

    is_datasource = row['content_type'] == 'publishedDatasources'

    camelcase_name = row['name'].lower()\
        .replace(' ', '_')\
        .replace('&', '_and_')\
        .replace('!', '_exclmtnpt_')\
        .replace('#', '_octothrp_')\
        .replace('$', '_dollarsgn_')\
        .replace('%', '_prcnt_')\
        .replace('"', '_prcnt_')\
        .replace("'", '_prcnt_')\
        .replace('(', '_parenlft_')\
        .replace(')', '_parenrght_')\
        .replace('*', '_astrsk_')\
        .replace('+', '_plus_')\
        .replace(',', '_comma_')\
        .replace('.', '_period_')\
        .replace('/', '_fwdslsh_')\
        .replace('\\', '_bckslsh_')\
        .replace(':', '_cln_')\
        .replace(';', '_semicln_')\
        .replace('<', '_anglbrcktlft_')\
        .replace('>', '_anglbrcktrght_')\
        .replace('=', '_eql_')\
        .replace('?', '_qstnmrk_')\
        .replace('@', '_atsgn_')\
        .replace('[', '_sqbrcktlft_')\
        .replace(']', '_sqbrcktrght_')\
        .replace('^', '_crcmflx_')\
        .replace('~', '_tilde_')\
        .replace('`', '_bcktck_')\
        .replace('{', '_sqrlbrcktlft_')\
        .replace('}', '_sqrlbrcktrght_')\
        .replace('|', '_pipesmbl_')

    def format_dependency(dependency):

        # Separate db, schema, table_or_view
        db, schema, table_or_view = dependency.split('.')

        # Prepare string for resource type
        if db in ['prod_reporting', 'prod_integration']:
            resource_type = 'ref'
        elif db == 'prod_raw':
            resource_type = 'source'
        else:
            resource_type = 'other'

        dependency = resource_type + "('" + schema + '_' + table_or_view + "')"
        return dependency

    # Populate dependencies
    dependencies = row['dependency_fullname'].split(',')
    dependencies = [format_dependency(x) for x in dependencies]
    dependencies = [x for x in dependencies if x != 'exclude']

    # Create exposure dictionary for this datasource / workbook
    exposure = {
        'name': camelcase_name,
        'label': row['name'],
        'url': 'https://10az.online.tableau.com/#/site/sunstate/'
               + ('datasources/' if is_datasource else 'workbooks/')
               + row['vizportalUrlId'],  # encountered an issue with Yard Mode datasource url, fix outstanding
        'type': 'published_datasource' if is_datasource else 'workbook',
        'owner': {'name': row['owner_name'], 'email': row['owner_email']},
        'depends_on': dependencies
    }

    exposure_contents.get('exposures').append(exposure)


def change_pandas_settings(max_columns, max_rows, max_colwidth, width):
    """
    Set Pandas display settings
    :param max_columns, max_rows, max_colwidth, width:
    :return None:
    """
    # Change Pandas settings:
    pd.set_option('display.max_columns', max_columns)
    pd.set_option('display.max_rows', max_rows)
    pd.set_option('display.max_colwidth', max_colwidth)
    pd.set_option('display.width', width)


if __name__ == "__main__":
    main()