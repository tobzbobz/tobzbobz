import gspread
from google.oauth2.service_account import Credentials
import os
import json

# Strikes Mapping (Column F for Non-Command, Column D for Command)
STRIKES_ROLES = {
    1432540488312950805: "Under Investigation",
    1365536207726973060: "Strike 3",
    1365536206892437545: "Strike 2",
    1365536206083067927: "Strike 1",
}

# Qualifications Mapping (Column I for Non-Command, Column C for Command)
QUALIFICATIONS_ROLES = {
    1412790680991961149: "Shift Qualified",
    1408256806417072188: "QFF Theory passed",
    1365539057374986382: "HAZMAT",
    1365538697604366418: "Technical Rescue",
    1411666839150395432: "OPS SUPPORT",
    1365538252039127101: "Rescue",
    1420021808060436702: "Qualified medical responder",  # Command only
}

# Rank Priority for Command sorting (lower number = higher priority)
COMMAND_RANK_PRIORITY = {
    "NC": 1,
    "DNC": 2,
    "ANC": 3,
    "AC": 4,
    "AAC": 5,
    "CO": 6,
    "DCO": 7,
    "SSO": 8,
    "SO": 9,
}

# Google Sheets Configuration
SPREADSHEET_ID = "1Qtb2xmrnDljsgL1wB-Yh2kPWZak7URjIfYueDKGCJ24"

# Define scopes
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

# Rank Classifications
NON_COMMAND_RANKS = {
    1309020834400047134: ("Recruit Firefighter", "RFF", 3),
    1309020730561790052: ("Qualified Firefighter", "QFF", 2),
    1309020647128825867: ("Senior Firefighter", "SFF", 1),
}

COMMAND_RANKS = {
    1309019405329502238: ("Station Officer", "SO"),
    1309019042765344810: ("Senior Station Officer", "SSO"),
    1365959865381556286: ("Deputy Chief Officer", "DCO"),
    1365959864618188880: ("Chief Officer", "CO"),
    1389158062635487312: ("Assistant Area Commander", "AAC"),
    1365959866363150366: ("Area Commander", "AC"),
    1389157690760232980: ("Assistant National Commander", "ANC"),
    1389157641799991347: ("Deputy National Commander", "DNC"),
    1285113945664917514: ("National Commander", "NC"),
}


class GoogleSheetsManager:
    def __init__(self):
        self.client = None
        self.spreadsheet = None
        self._cached_validations = {}  # Cache for dropdown validations

    def authenticate(self):
        """Authenticate with Google Sheets API using environment variables"""
        try:
            # Get service account credentials from environment variable
            service_account_info = os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON')

            if not service_account_info:
                print("❌ GOOGLE_SERVICE_ACCOUNT_JSON environment variable not set")
                return False

            # Parse the JSON string
            service_account_dict = json.loads(service_account_info)

            # Create credentials from the dictionary
            creds = Credentials.from_service_account_info(
                service_account_dict,
                scopes=SCOPES
            )

            self.client = gspread.authorize(creds)
            self.spreadsheet = self.client.open_by_key(SPREADSHEET_ID)
            print("✅ Google Sheets authenticated successfully")
            return True
        except json.JSONDecodeError as e:
            print(f"❌ Invalid JSON in GOOGLE_SERVICE_ACCOUNT_JSON: {e}")
            return False
        except Exception as e:
            print(f"❌ Google Sheets authentication failed: {e}")
            return False

    def get_worksheet(self, sheet_name: str):
        """Get a specific worksheet by name"""
        try:
            return self.spreadsheet.worksheet(sheet_name)
        except Exception as e:
            print(f"❌ Error getting worksheet '{sheet_name}': {e}")
            return None

    def find_first_empty_row(self, worksheet) -> int:
        """Find the first completely empty row in the worksheet"""
        try:
            # Get all values
            all_values = worksheet.get_all_values()

            # Find first empty row
            for i, row in enumerate(all_values, start=1):
                if all(cell == '' for cell in row):
                    return i

            # If no empty row found, return next row after last
            return len(all_values) + 1
        except Exception as e:
            print(f"❌ Error finding empty row: {e}")
            return 2  # Default to row 2 if error

    def find_row_by_discord_id(self, worksheet, discord_id: str, column: str) -> int:
        """Find row number by Discord ID in specified column"""
        try:
            cell = worksheet.find(str(discord_id), in_column=self._column_to_number(column))
            return cell.row if cell else None
        except Exception as e:
            print(f"❌ Error finding Discord ID: {e}")
            return None

    def _column_to_number(self, column: str) -> int:
        """Convert column letter to number (A=1, B=2, etc.)"""
        return ord(column.upper()) - ord('A') + 1

    def _column_to_letter(self, column: int) -> str:
        """Convert column number to letter (1=A, 2=B, etc.)"""
        return chr(ord('A') + column - 1)

    def delete_row(self, worksheet, row_number: int):
        """Delete a specific row"""
        try:
            worksheet.delete_rows(row_number)
            print(f"✅ Deleted row {row_number} from {worksheet.title}")
        except Exception as e:
            print(f"❌ Error deleting row: {e}")

    def get_existing_data_validation(self, worksheet, row: int, column: int):
        """
        Get existing data validation rules from a cell to reuse them
        Returns the validation options as a list, or None if no validation exists
        """
        try:
            cache_key = f"{worksheet.title}_{column}"

            # Return cached validation if available
            if cache_key in self._cached_validations:
                return self._cached_validations[cache_key]

            worksheet_id = worksheet.id

            # Get sheet metadata to find data validation rules
            sheet_metadata = self.spreadsheet.fetch_sheet_metadata()

            for sheet in sheet_metadata['sheets']:
                if sheet['properties']['sheetId'] == worksheet_id:
                    # Look for conditionalFormats or dataValidation in the sheet
                    if 'conditionalFormats' in sheet:
                        for rule in sheet['conditionalFormats']:
                            if 'dataValidation' in rule:
                                # Check if this rule applies to our column
                                rule_range = rule.get('ranges', [{}])[0]
                                rule_col = rule_range.get('startColumnIndex', -1)

                                if rule_col == column - 1:  # Convert to 0-based
                                    condition = rule['dataValidation'].get('condition', {})
                                    if condition.get('type') == 'ONE_OF_LIST':
                                        values = [v.get('userEnteredValue') for v in condition.get('values', [])]
                                        self._cached_validations[cache_key] = values
                                        return values

            # If no validation found, try to get it from another row in the same column
            # Look at row 2 (first data row after header)
            try:
                column_letter = self._column_to_letter(column)
                cell_range = f"{column_letter}2"

                # This is a workaround - we'll check if row 2 has validation
                # If it does, we assume all rows in this column should have the same validation
                print(f"ℹ️ No cached validation found for {worksheet.title} column {column}, checking row 2...")

            except:
                pass

            return None

        except Exception as e:
            print(f"⚠️ Could not get existing data validation: {e}")
            return None

    def copy_data_validation_to_cell(self, worksheet, source_row: int, target_row: int, column: int):
        """
        Copy data validation from one cell to another in the same column
        This preserves existing dropdown configurations
        """
        try:
            worksheet_id = worksheet.id
            column_letter = self._column_to_letter(column)

            # Use copyPaste request to copy validation
            copy_request = {
                "copyPaste": {
                    "source": {
                        "sheetId": worksheet_id,
                        "startRowIndex": source_row - 1,
                        "endRowIndex": source_row,
                        "startColumnIndex": column - 1,
                        "endColumnIndex": column
                    },
                    "destination": {
                        "sheetId": worksheet_id,
                        "startRowIndex": target_row - 1,
                        "endRowIndex": target_row,
                        "startColumnIndex": column - 1,
                        "endColumnIndex": column
                    },
                    "pasteType": "PASTE_DATA_VALIDATION"
                }
            }

            self.spreadsheet.batch_update({"requests": [copy_request]})
            print(f"✅ Copied validation to {column_letter}{target_row}")
            return True

        except Exception as e:
            print(f"⚠️ Could not copy data validation: {e}")
            return False

    def determine_rank_type(self, member_roles) -> tuple:
        """
        Determine if user is Non-Command or Command
        Returns: (rank_type, rank_data) where rank_type is 'non-command' or 'command'
        """
        # Check for command ranks first (they take priority)
        for role in member_roles:
            if role.id in COMMAND_RANKS:
                return ('command', COMMAND_RANKS[role.id])

        # Check for non-command ranks
        for role in member_roles:
            if role.id in NON_COMMAND_RANKS:
                return ('non-command', NON_COMMAND_RANKS[role.id])

        return (None, None)

    def determine_strikes_value(self, member_roles) -> str:
        """
        Determine strikes value based on roles (priority: Under Investigation > Strike 3 > Strike 2 > Strike 1 > Clear)
        """
        # Check in priority order
        priority_order = [
            (1432540488312950805, "Under Investigation"),
            (1365536207726973060, "Strike 3"),
            (1365536206892437545, "Strike 2"),
            (1365536206083067927, "Strike 1"),
        ]

        for role_id, value in priority_order:
            if any(role.id == role_id for role in member_roles):
                return value

        # Default to "Clear" for Non-Command or "Good Boy" for Command
        return None  # Will be set based on rank type

    def determine_qualifications(self, member_roles, is_command: bool = False) -> str:
        """
        Determine qualifications based on roles (can have multiple)
        Returns comma-separated string of qualifications
        """
        qualifications = []

        for role in member_roles:
            if role.id in QUALIFICATIONS_ROLES:
                qual = QUALIFICATIONS_ROLES[role.id]
                # Skip "Qualified medical responder" if not command
                if qual == "Qualified medical responder" and not is_command:
                    continue
                qualifications.append(qual)

        if not qualifications:
            return "No additional qualifications"

        # Return comma-separated list
        return ", ".join(qualifications)

    async def add_callsign_to_sheets(self, member, callsign: str, fenz_prefix: str,
                                     roblox_username: str, discord_id: int):
        """
        Add or update callsign in Google Sheets, preserving existing dropdown validation
        """
        try:
            if not self.client:
                auth_success = self.authenticate()
                if not auth_success:
                    return False

            # Determine rank type
            rank_type, rank_data = self.determine_rank_type(member.roles)

            if not rank_type:
                print(f"⚠️ No FENZ rank found for {member.display_name}")
                return False

            # Get both worksheets
            non_command_sheet = self.get_worksheet("Non-Command")
            command_sheet = self.get_worksheet("Command")

            if not non_command_sheet or not command_sheet:
                print("❌ Could not access worksheets")
                return False

            # Search for existing entry in both sheets (Column G for Non-Command, Column E for Command)
            existing_non_command_row = self.find_row_by_discord_id(non_command_sheet, discord_id, 'G')
            existing_command_row = self.find_row_by_discord_id(command_sheet, discord_id, 'E')

            # Handle rank transitions
            if rank_type == 'non-command':
                # Delete from Command sheet if exists (rank transition)
                if existing_command_row:
                    self.delete_row(command_sheet, existing_command_row)
            elif rank_type == 'command':
                # Delete from Non-Command sheet if exists (rank transition)
                if existing_non_command_row:
                    self.delete_row(non_command_sheet, existing_non_command_row)

            if rank_type == 'non-command':
                # Non-Command: A, B, C, D, F, G, H, I
                target_sheet = non_command_sheet

                # Use existing row if found, otherwise find empty row
                if existing_non_command_row:
                    empty_row = existing_non_command_row
                    print(f"ℹ️ Updating existing Non-Command row {empty_row}")
                else:
                    empty_row = self.find_first_empty_row(target_sheet)
                    print(f"ℹ️ Creating new Non-Command row {empty_row}")

                rank_name, rank_prefix, rank_number = rank_data
                full_callsign = f"{fenz_prefix}-{callsign}"

                # Determine strikes and qualifications
                strikes_value = self.determine_strikes_value(member.roles)
                if strikes_value is None:
                    strikes_value = "Clear"  # Default for Non-Command
                qualifications = self.determine_qualifications(member.roles, is_command=False)

                # Update cells with values
                target_sheet.update_cell(empty_row, 1, full_callsign)  # A: Full callsign
                target_sheet.update_cell(empty_row, 2, fenz_prefix)  # B: FENZ Prefix
                target_sheet.update_cell(empty_row, 3, callsign)  # C: Callsign number
                target_sheet.update_cell(empty_row, 4, roblox_username)  # D: Roblox username
                target_sheet.update_cell(empty_row, 6, strikes_value)  # F: Strikes
                target_sheet.update_cell(empty_row, 7, str(discord_id))  # G: Discord ID
                target_sheet.update_cell(empty_row, 8, rank_number)  # H: Rank number
                target_sheet.update_cell(empty_row, 9, qualifications)  # I: Qualifications

                # Copy data validation from row 2 (template row) to preserve existing dropdowns
                # Column F: Strikes dropdown
                self.copy_data_validation_to_cell(target_sheet, source_row=2, target_row=empty_row, column=6)

                # Column I: Qualifications dropdown
                self.copy_data_validation_to_cell(target_sheet, source_row=2, target_row=empty_row, column=9)

                print(f"✅ Added Non-Command callsign {full_callsign} to row {empty_row}")

            else:  # command
                # Command: A, B, C, D, E, F
                target_sheet = command_sheet

                # Use existing row if found, otherwise find empty row
                if existing_command_row:
                    empty_row = existing_command_row
                    print(f"ℹ️ Updating existing Command row {empty_row}")
                else:
                    empty_row = self.find_first_empty_row(target_sheet)
                    print(f"ℹ️ Creating new Command row {empty_row}")

                rank_name, rank_prefix = rank_data
                full_callsign = f"{fenz_prefix}-{callsign}"

                # Determine strikes and qualifications
                strikes_value = self.determine_strikes_value(member.roles)
                if strikes_value is None:
                    strikes_value = "Good Boy"
                qualifications = self.determine_qualifications(member.roles, is_command=True)

                # Get rank priority for sorting
                rank_priority = COMMAND_RANK_PRIORITY.get(fenz_prefix, 99)  # Default to 99 if unknown

                # Update cells with values
                target_sheet.update_cell(empty_row, 1, full_callsign)  # A: Full callsign
                target_sheet.update_cell(empty_row, 2, roblox_username)  # B: Roblox username
                target_sheet.update_cell(empty_row, 3, qualifications)  # C: Qualifications
                target_sheet.update_cell(empty_row, 4, strikes_value)  # D: Strikes
                target_sheet.update_cell(empty_row, 5, str(discord_id))  # E: Discord ID
                target_sheet.update_cell(empty_row, 6, rank_priority)  # F: Rank priority

                # Copy data validation from row 2 (template row) to preserve existing dropdowns
                # Column D: Strikes dropdown
                self.copy_data_validation_to_cell(target_sheet, source_row=2, target_row=empty_row, column=4)

                # Column C: Qualifications dropdown
                self.copy_data_validation_to_cell(target_sheet, source_row=2, target_row=empty_row, column=3)

                print(f"✅ Added Command callsign {full_callsign} to row {empty_row}")

            return True

        except Exception as e:
            print(f"❌ Error adding to Google Sheets: {e}")
            import traceback
            traceback.print_exc()
            return False

    def sort_worksheet_multi(self, worksheet, sort_specs: list):
        """
        Sort a worksheet by multiple columns
        sort_specs: List of dicts with 'column' (1-based) and 'order' ('ASCENDING' or 'DESCENDING')
        Example: [{'column': 8, 'order': 'ASCENDING'}, {'column': 3, 'order': 'ASCENDING'}]
        """
        try:
            worksheet_id = worksheet.id

            # Build sort specs for API
            api_sort_specs = []
            for spec in sort_specs:
                api_sort_specs.append({
                    'dimensionIndex': spec['column'] - 1,  # Convert to 0-based
                    'sortOrder': spec['order']
                })

            sort_request = {
                'sortRange': {
                    'range': {
                        'sheetId': worksheet_id,
                        'startRowIndex': 1,  # Skip header row
                    },
                    'sortSpecs': api_sort_specs
                }
            }

            self.spreadsheet.batch_update({'requests': [sort_request]})
            print(f"✅ Sorted {worksheet.title}")
            return True
        except Exception as e:
            print(f"❌ Error sorting worksheet: {e}")
            return False

    def detect_rank_mismatch(self, member_roles, current_fenz_prefix: str) -> tuple:
        """
        Check if user's current prefix matches their Discord roles
        Returns: (has_mismatch: bool, correct_prefix: str, correct_rank_type: str)
        """
        rank_type, rank_data = self.determine_rank_type(member_roles)

        if not rank_type:
            return (False, None, None)

        if rank_type == 'non-command':
            _, correct_prefix, _ = rank_data
        else:  # command
            _, correct_prefix = rank_data

        # Check if current prefix matches correct prefix
        has_mismatch = (current_fenz_prefix != correct_prefix)

        return (has_mismatch, correct_prefix, rank_type)

    async def batch_update_callsigns(self, callsign_data: list):
        """
        Batch update all callsigns to Google Sheets using efficient batch operations
        callsign_data: List of dicts with callsign info
        """
        try:
            if not self.client:
                auth_success = self.authenticate()
                if not auth_success:
                    return False

            # Get both worksheets
            non_command_sheet = self.get_worksheet("Non-Command")
            command_sheet = self.get_worksheet("Command")

            if not non_command_sheet or not command_sheet:
                print("❌ Could not access worksheets")
                return False

            # Clear existing data (keep headers)
            print("🗑️ Clearing existing data...")

            # Clear Non-Command sheet (starting from row 2)
            non_command_values = non_command_sheet.get_all_values()
            if len(non_command_values) > 1:
                non_command_sheet.delete_rows(2, len(non_command_values))

            # Clear Command sheet (starting from row 2)
            command_values = command_sheet.get_all_values()
            if len(command_values) > 1:
                command_sheet.delete_rows(2, len(command_values))

            print("✅ Sheets cleared, preparing batch data...")

            # Separate data into command and non-command
            command_rows = []
            non_command_rows = []

            for data in callsign_data:
                fenz_prefix = data['fenz_prefix']

                # Check if it's a command rank
                is_command = any(fenz_prefix == prefix for _, (_, prefix) in COMMAND_RANKS.items())

                if is_command:
                    full_callsign = f"{fenz_prefix}-{data['callsign']}" if fenz_prefix else data['callsign']
                    rank_priority = COMMAND_RANK_PRIORITY.get(fenz_prefix, 99)

                    command_rows.append([
                        full_callsign,
                        data['roblox_username'],
                        "No additional qualifications",
                        "Good Boy",
                        str(data['discord_user_id']),
                        rank_priority
                    ])
                else:
                    full_callsign = f"{fenz_prefix}-{data['callsign']}"
                    rank_number = 3
                    for role_id, (_, prefix, num) in NON_COMMAND_RANKS.items():
                        if prefix == fenz_prefix:
                            rank_number = num
                            break

                    non_command_rows.append([
                        full_callsign,
                        fenz_prefix,
                        data['callsign'],
                        data['roblox_username'],
                        "",
                        "Clear",
                        str(data['discord_user_id']),
                        rank_number,
                        "No additional qualifications"
                    ])

            # BATCH UPDATE: Write all rows at once
            if command_rows:
                print(f"📝 Writing {len(command_rows)} command callsigns...")
                command_sheet.update('A2', command_rows, value_input_option='RAW')

                # BATCHED VALIDATION COPY - Single API call for all rows
                print("📋 Copying validations for command sheet (batched)...")
                self.batch_copy_validations(
                    command_sheet,
                    source_row=2,
                    target_rows=range(2, 2 + len(command_rows)),
                    columns=[3, 4]  # Columns C and D
                )

            if non_command_rows:
                print(f"📝 Writing {len(non_command_rows)} non-command callsigns...")
                non_command_sheet.update('A2', non_command_rows, value_input_option='RAW')

                # BATCHED VALIDATION COPY - Single API call for all rows
                print("📋 Copying validations for non-command sheet (batched)...")
                self.batch_copy_validations(
                    non_command_sheet,
                    source_row=2,
                    target_rows=range(2, 2 + len(non_command_rows)),
                    columns=[6, 9]  # Columns F and I
                )

            # Sort both sheets
            print("📊 Sorting sheets...")
            self.sort_worksheet_multi(non_command_sheet, [
                {'column': 8, 'order': 'ASCENDING'},
                {'column': 3, 'order': 'ASCENDING'}
            ])
            self.sort_worksheet_multi(command_sheet, [
                {'column': 6, 'order': 'ASCENDING'}
            ])

            print(f"✅ Batch update complete: {len(callsign_data)} callsigns synced")
            return True

        except Exception as e:
            print(f"❌ Error in batch update: {e}")
            import traceback
            traceback.print_exc()
            return False

    def batch_copy_validations(self, worksheet, source_row: int, target_rows: range, columns: list):
        """
        Copy data validations from source row to multiple target rows in a single batch request
        This dramatically reduces API calls and avoids rate limits

        Args:
            worksheet: The worksheet to update
            source_row: Row to copy validation from (usually row 2)
            target_rows: Range of rows to copy to (e.g., range(2, 50))
            columns: List of column numbers to copy validations for (e.g., [3, 4])
        """
        try:
            worksheet_id = worksheet.id
            requests = []

            for column in columns:
                for target_row in target_rows:
                    # Skip if copying to itself
                    if target_row == source_row:
                        continue

                    requests.append({
                        "copyPaste": {
                            "source": {
                                "sheetId": worksheet_id,
                                "startRowIndex": source_row - 1,
                                "endRowIndex": source_row,
                                "startColumnIndex": column - 1,
                                "endColumnIndex": column
                            },
                            "destination": {
                                "sheetId": worksheet_id,
                                "startRowIndex": target_row - 1,
                                "endRowIndex": target_row,
                                "startColumnIndex": column - 1,
                                "endColumnIndex": column
                            },
                            "pasteType": "PASTE_DATA_VALIDATION"
                        }
                    })

            # Execute all requests in a single batch (limit to 100 requests per batch)
            batch_size = 100
            for i in range(0, len(requests), batch_size):
                batch = requests[i:i + batch_size]
                self.spreadsheet.batch_update({"requests": batch})
                print(f"✅ Copied validations (batch {i // batch_size + 1}/{(len(requests) - 1) // batch_size + 1})")

            return True

        except Exception as e:
            print(f"⚠️ Could not batch copy data validation: {e}")
            return False
    
    async def get_all_callsigns(self):
        """
        Get all existing callsigns from both sheets
        Returns: list of dicts with callsign data
        """
        try:
            if not self.client:
                auth_success = self.authenticate()
                if not auth_success:
                    return []

            all_callsigns = []

            # Get Non-Command sheet data
            non_command_sheet = self.get_worksheet("Non-Command")
            if non_command_sheet:
                non_command_data = non_command_sheet.get_all_values()
                for row in non_command_data[1:]:  # Skip header
                    if row and len(row) >= 7 and row[6]:  # Check Discord ID exists
                        all_callsigns.append({
                            'full_callsign': row[0],
                            'fenz_prefix': row[1],
                            'callsign': row[2],
                            'roblox_username': row[3],
                            'discord_user_id': row[6],
                            'sheet': 'Non-Command'
                        })

            # Get Command sheet data
            command_sheet = self.get_worksheet("Command")
            if command_sheet:
                command_data = command_sheet.get_all_values()
                for row in command_data[1:]:  # Skip header
                    if row and len(row) >= 5 and row[4]:  # Check Discord ID exists
                        all_callsigns.append({
                            'full_callsign': row[0],
                            'roblox_username': row[1],
                            'discord_user_id': row[4],
                            'sheet': 'Command'
                        })

            return all_callsigns

        except Exception as e:
            print(f"❌ Error getting callsigns: {e}")
            return []


# Create global instance
sheets_manager = GoogleSheetsManager()