# 🎯 EXCEL SHEET EXPLORER - HANDLES MULTIPLE SHEETS!
import pandas as pd
import os

print("🔮 Welcome to your Excel Sheet Explorer!")
print("=" * 50)

# 🗂️ Let's see what Excel files you have
data_folder = "data/raw/"
print(f"📁 Looking in folder: {data_folder}")

# 👀 Check what files are there
if os.path.exists(data_folder):
    files = os.listdir(data_folder)
    excel_files = [f for f in files if f.endswith(('.xlsx', '.xls'))]
    
    if excel_files:
        print("✅ Found these Excel files:")
        for i, file in enumerate(excel_files, 1):
            print(f"   {i}. {file}")
        
        # 📖 Read the first Excel file
        first_file = excel_files[0]
        file_path = os.path.join(data_folder, first_file)
        
        print(f"\n📊 Reading: {first_file}")
        
        # 🎯 NEW: SEE ALL SHEETS FIRST!
        excel_file = pd.ExcelFile(file_path)
        sheet_names = excel_file.sheet_names
        
        print(f"\n📑 Found {len(sheet_names)} sheets:")
        for i, sheet in enumerate(sheet_names, 1):
            print(f"   {i}. '{sheet}'")
        
        # 🔍 LET'S EXPLORE EACH SHEET!
        print(f"\n" + "="*50)
        print("🔍 EXPLORING EACH SHEET:")
        print("="*50)
        
        for sheet_name in sheet_names:
            print(f"\n📖 Sheet: '{sheet_name}'")
            print("-" * 30)
            
            # Read this specific sheet
            sheet_data = pd.read_excel(file_path, sheet_name=sheet_name)
            
            # Show sheet info
            print(f"   Rows: {len(sheet_data)}, Columns: {len(sheet_data.columns)}")
            print(f"   Column names: {list(sheet_data.columns)}")
            
            # Show first 3 rows
            print(f"\n   First 3 rows:")
            print(sheet_data.head(3))
            
            # 💾 Save each sheet as separate CSV
            clean_folder = "data/clean/"
            os.makedirs(clean_folder, exist_ok=True)
            
            # Create nice filename
            csv_filename = f"{first_file.replace('.xlsx', '').replace('.xls', '')}_{sheet_name}.csv"
            csv_path = os.path.join(clean_folder, csv_filename)
            
            sheet_data.to_csv(csv_path, index=False)
            print(f"   💾 Saved as: {csv_filename}")
        
        print(f"\n🎉 SUCCESS! All sheets processed!")
        print("📂 Check your 'data/clean/' folder for all the CSV files!")
        
    else:
        print("❌ No Excel files found in data/raw/")
else:
    print("❌ data/raw/ folder doesn't exist!")