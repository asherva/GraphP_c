import requests
import json
import pandas as pd
from typing import Dict, List, Optional
import xml.etree.ElementTree as ET

class TableauGraphQLExtractor:
    def __init__(self, server_url: str, api_version: str = "3.19"):
        """
        אתחול המחלקה לחילוץ מידע מ-Tableau Server
        
        Args:
            server_url: כתובת שרת הטאבלו (לדוגמה: https://your-server.com)
            api_version: גרסת ה-API
        """
        self.server_url = server_url.rstrip('/')
        self.api_version = api_version
        self.auth_token = None
        self.site_id = None
        self.session = requests.Session()
        
    def authenticate(self, username: str, password: str, site_name: str = "") -> bool:
        """
        התחברות לשרת הטאבלו
        
        Args:
            username: שם משתמש
            password: סיסמה
            site_name: שם האתר (אופציונלי)
            
        Returns:
            True אם ההתחברות הצליחה
        """
        auth_url = f"{self.server_url}/api/{self.api_version}/auth/signin"
        
        # יצירת XML לבקשת התחברות
        signin_xml = f"""
        <tsRequest>
            <credentials name="{username}" password="{password}">
                <site contentUrl="{site_name}"/>
            </credentials>
        </tsRequest>
        """
        
        headers = {'Content-Type': 'application/xml'}
        
        try:
            response = self.session.post(auth_url, data=signin_xml, headers=headers)
            response.raise_for_status()
            
            # חילוץ טוקן אימות מה-XML
            root = ET.fromstring(response.content)
            credentials = root.find('.//credentials')
            if credentials is not None:
                self.auth_token = credentials.get('token')
                site = root.find('.//site')
                if site is not None:
                    self.site_id = site.get('id')
                
                # הוספת הטוקן לכל הבקשות הבאות
                self.session.headers.update({
                    'X-Tableau-Auth': self.auth_token,
                    'Content-Type': 'application/json'
                })
                return True
        except Exception as e:
            print(f"שגיאה בהתחברות: {e}")
            return False
        
        return False
    
    def execute_graphql_query(self, query: str) -> Optional[Dict]:
        """
        ביצוע שאילתת GraphQL
        
        Args:
            query: שאילתת GraphQL
            
        Returns:
            תוצאות השאילתה או None במקרה של שגיאה
        """
        if not self.auth_token:
            print("נדרשת התחברות תחילה")
            return None
            
        graphql_url = f"{self.server_url}/api/metadata/graphql"
        
        payload = {
            "query": query
        }
        
        try:
            response = self.session.post(graphql_url, json=payload)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"שגיאה בביצוע שאילתת GraphQL: {e}")
            return None
    
    def get_workbooks_and_queries(self) -> List[Dict]:
        """
        קבלת רשימת דוחות עם פרטי השאילתות שלהם
        
        Returns:
            רשימה של דיקטים עם מידע על הדוחות
        """
        query = """
        {
            workbooks {
                id
                name
                description
                createdAt
                updatedAt
                owner {
                    username
                }
                sheets {
                    id
                    name
                    datasources {
                        id
                        name
                        connectionType
                        tables {
                            id
                            name
                            schema
                            fullName
                        }
                        customSQLTables {
                            id
                            name
                            query
                        }
                    }
                }
                embeddedDatasources {
                    id
                    name
                    connectionType
                    tables {
                        id
                        name
                        schema
                        fullName
                    }
                    customSQLTables {
                        id
                        name
                        query
                    }
                }
            }
        }
        """
        
        result = self.execute_graphql_query(query)
        if result and 'data' in result:
            return self.process_workbook_data(result['data']['workbooks'])
        return []
    
    def get_published_datasources(self) -> List[Dict]:
        """
        קבלת רשימת מקורות נתונים מפורסמים
        
        Returns:
            רשימה של דיקטים עם מידע על מקורות הנתונים
        """
        query = """
        {
            publishedDatasources {
                id
                name
                description
                connectionType
                createdAt
                updatedAt
                owner {
                    username
                }
                tables {
                    id
                    name
                    schema
                    fullName
                }
                customSQLTables {
                    id
                    name
                    query
                }
            }
        }
        """
        
        result = self.execute_graphql_query(query)
        if result and 'data' in result:
            return self.process_datasource_data(result['data']['publishedDatasources'])
        return []
    
    def process_workbook_data(self, workbooks: List[Dict]) -> List[Dict]:
        """
        עיבוד נתונים של דוחות
        """
        processed_data = []
        
        for workbook in workbooks:
            base_info = {
                'report_type': 'Workbook',
                'report_id': workbook['id'],
                'report_name': workbook['name'],
                'description': workbook.get('description', ''),
                'owner': workbook.get('owner', {}).get('username', ''),
                'created_at': workbook.get('createdAt', ''),
                'updated_at': workbook.get('updatedAt', '')
            }
            
            # עיבוד datasources מוטבעים
            for datasource in workbook.get('embeddedDatasources', []):
                self.process_datasource_info(datasource, base_info, processed_data, 'Embedded')
            
            # עיבוד sheets
            for sheet in workbook.get('sheets', []):
                sheet_info = base_info.copy()
                sheet_info['sheet_name'] = sheet['name']
                sheet_info['sheet_id'] = sheet['id']
                
                for datasource in sheet.get('datasources', []):
                    self.process_datasource_info(datasource, sheet_info, processed_data, 'Sheet')
                
                # אם אין datasources ברמת השיט, הוסף רק את מידע השיט
                if not sheet.get('datasources'):
                    processed_data.append(sheet_info)
        
        return processed_data
    
    def process_datasource_data(self, datasources: List[Dict]) -> List[Dict]:
        """
        עיבוד נתונים של מקורות נתונים מפורסמים
        """
        processed_data = []
        
        for datasource in datasources:
            base_info = {
                'report_type': 'Published Datasource',
                'report_id': datasource['id'],
                'report_name': datasource['name'],
                'description': datasource.get('description', ''),
                'owner': datasource.get('owner', {}).get('username', ''),
                'created_at': datasource.get('createdAt', ''),
                'updated_at': datasource.get('updatedAt', '')
            }
            
            self.process_datasource_info(datasource, base_info, processed_data, 'Published')
        
        return processed_data
    
    def process_datasource_info(self, datasource: Dict, base_info: Dict, 
                               processed_data: List[Dict], datasource_type: str):
        """
        עיבוד מידע על מקור נתונים ספציפי
        """
        ds_info = base_info.copy()
        ds_info.update({
            'datasource_type': datasource_type,
            'datasource_name': datasource['name'],
            'datasource_id': datasource['id'],
            'connection_type': datasource.get('connectionType', '')
        })
        
        # עיבוד טבלאות רגילות
        for table in datasource.get('tables', []):
            table_info = ds_info.copy()
            table_info.update({
                'table_name': table['name'],
                'table_id': table['id'],
                'schema_name': table.get('schema', ''),
                'full_table_name': table.get('fullName', ''),
                'sql_query': '',
                'is_custom_sql': False
            })
            processed_data.append(table_info)
        
        # עיבוד טבלאות SQL מותאמות
        for custom_table in datasource.get('customSQLTables', []):
            custom_info = ds_info.copy()
            custom_info.update({
                'table_name': custom_table['name'],
                'table_id': custom_table['id'],
                'schema_name': '',
                'full_table_name': '',
                'sql_query': custom_table.get('query', ''),
                'is_custom_sql': True
            })
            processed_data.append(custom_info)
        
        # אם אין טבלאות, הוסף רק את מידע מקור הנתונים
        if not datasource.get('tables') and not datasource.get('customSQLTables'):
            processed_data.append(ds_info)
    
    def save_to_excel(self, data: List[Dict], filename: str = "tableau_reports_data.xlsx"):
        """
        שמירת הנתונים לקובץ Excel
        """
        if not data:
            print("אין נתונים לשמירה")
            return
        
        df = pd.DataFrame(data)
        
        # סידור העמודות
        columns_order = [
            'report_type', 'report_name', 'report_id', 'description', 'owner',
            'sheet_name', 'sheet_id', 'datasource_type', 'datasource_name', 
            'datasource_id', 'connection_type', 'table_name', 'table_id',
            'schema_name', 'full_table_name', 'sql_query', 'is_custom_sql',
            'created_at', 'updated_at'
        ]
        
        # סידור העמודות (רק אלו שקיימות)
        existing_columns = [col for col in columns_order if col in df.columns]
        df = df[existing_columns]
        
        # שמירה לאקסל
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Tableau Reports Data', index=False)
            
            # התאמת רוחב העמודות
            worksheet = writer.sheets['Tableau Reports Data']
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)  # מקסימום 50 תווים
                worksheet.column_dimensions[column_letter].width = adjusted_width
        
        print(f"הנתונים נשמרו בהצלחה לקובץ: {filename}")
    
    def print_summary(self, data: List[Dict]):
        """
        הדפסת סיכום הנתונים
        """
        if not data:
            print("לא נמצאו נתונים")
            return
        
        df = pd.DataFrame(data)
        
        print("\n=== סיכום נתונים ===")
        print(f"סך הכל רשומות: {len(df)}")
        
        if 'report_type' in df.columns:
            print("\nפירוט לפי סוג דוח:")
            print(df['report_type'].value_counts().to_string())
        
        if 'connection_type' in df.columns:
            print("\nפירוט לפי סוג חיבור:")
            print(df['connection_type'].value_counts().to_string())
        
        if 'is_custom_sql' in df.columns:
            custom_sql_count = df['is_custom_sql'].sum() if df['is_custom_sql'].dtype == bool else 0
            print(f"\nמספר שאילתות SQL מותאמות: {custom_sql_count}")
    
    def logout(self):
        """
        התנתקות מהשרת
        """
        if self.auth_token:
            logout_url = f"{self.server_url}/api/{self.api_version}/auth/signout"
            try:
                self.session.post(logout_url)
            except:
                pass
            finally:
                self.auth_token = None
                self.site_id = None

def main():
    """
    פונקציה ראשית להרצת הסקריפט
    """
    # הגדרות חיבור
    SERVER_URL = "https://your-tableau-server.com"  # החלף עם כתובת השרת שלך
    USERNAME = "your-username"  # החלף עם שם המשתמש שלך
    PASSWORD = "your-password"  # החלף עם הסיסמה שלך
    SITE_NAME = ""  # השאר ריק עבור האתר הברירת מחדל
    
    # יצירת מופע של המחלקה
    extractor = TableauGraphQLExtractor(SERVER_URL)
    
    try:
        # התחברות
        print("מתחבר לשרת Tableau...")
        if not extractor.authenticate(USERNAME, PASSWORD, SITE_NAME):
            print("שגיאה בהתחברות לשרת")
            return
        
        print("התחברות הצליחה!")
        
        # חילוץ נתונים
        print("מחלץ מידע על דוחות...")
        workbook_data = extractor.get_workbooks_and_queries()
        
        print("מחלץ מידע על מקורות נתונים מפורסמים...")
        datasource_data = extractor.get_published_datasources()
        
        # איחוד הנתונים
        all_data = workbook_data + datasource_data
        
        # הדפסת סיכום
        extractor.print_summary(all_data)
        
        # שמירה לאקסל
        if all_data:
            extractor.save_to_excel(all_data)
        else:
            print("לא נמצאו נתונים לשמירה")
            
    except Exception as e:
        print(f"שגיאה כללית: {e}")
    
    finally:
        # התנתקות
        extractor.logout()
        print("התנתקות הושלמה")

if __name__ == "__main__":
    main()
