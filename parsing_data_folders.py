""" Functions to process and parse through folders/files used for training the model. 
Some of the folder come from One Drive and they are uniformly formatted  and nested as:
<study_folder>  -> <subfolders> -> <fcs> and <pdf> files.
Some folder come from RedCap and they are downloaded as .zip files and their formatting is not universal. So for each study downloaded from RedCap
we implemented a small function,
that normalizes the formatting"""

"""Function for Normalizing the format of Abbvie Studies"""
from pathlib import Path
import pandas as pd

def rename_abbvie_files(abbvie_report: pd.DataFrame,
                        root: Path,
                        dry_run: bool = True) -> None:
    """
    Rename AbbVie balance / whole‑blood files inside *root* based on filenames
    stored in `abbvie_report`.

    Parameters
    ----------
    abbvie_report : pd.DataFrame
        Must contain columns like 'bal1_sort_report', 'bal2_sort_fcs',
        and 'whole_blood_fcs_1' plus the usual metadata columns:
        'study_code' and 'redcap_event_name'.
    root : pathlib.Path
        Folder where the files live.
    dry_run : bool, default True
        If True, only prints what *would* happen.  Set False to rename for real.
    """
    for _, row in abbvie_report.iterrows():
        study = row.study_code
        event = row.redcap_event_name

        for bal in (1, 2):
            for kind, ext in (('sort_report', 'pdf'),
                              ('sort_fcs',    'fcs')):
                # grab both “new” names
                main_name  = str(row.get(f'bal{bal}_{kind}', '')).strip()
                blood_name = str(row.get('whole_blood_fcs_1', '')).strip()

                # (search‑pattern, target‑name) pairs
                candidates = [
                    (f"{study}_{event}_bal{bal}_{kind}.{ext}", main_name),
                    (f"{study}_{event}_whole_blood_fcs_1.{ext}", blood_name)
                ]

                for stub, new_name in candidates:
                    if not new_name or new_name.lower() == 'nan':
                        continue  # nothing to rename to

                    for old_path in root.glob(stub):
                        new_path = root / new_name
                        print(f"{old_path.name}  →  {new_path.name}")
                        if not dry_run:
                            old_path.rename(new_path)

"""Function for parsing Big Study Folders from OneDrive"""

def parse_fcs_folder(main_folder):
  """Takes in main_folder - big study folder that contains
  subfolder for each month"""
  
    study_condition_map = {
        'PASC': 'long COVID (Post-acute sequelae of SARS-CoV-2 infection)',
        'Abbvie': 'scleroderma',
        'Duke_ozone': 'control samples, healthy volunteers',
        'WashU_BAL': 'pneumonia',
        'SCRIPT': 'pneumonia'
    }  #add more studies if needed

    records = []

    for subfolder in os.listdir(main_folder):
        subfolder_path = os.path.join(main_folder, subfolder)
        if not os.path.isdir(subfolder_path):
            continue

        study_name = os.path.basename(main_folder)
        condition = study_condition_map.get(study_name, 'Unknown')

        fcs_files = [f for f in os.listdir(subfolder_path) if f.endswith('.fcs')]
        pdf_files = [f for f in os.listdir(subfolder_path) if f.endswith('.pdf')]

        for fcs in fcs_files:
            fcs_parts = fcs.split('_')
            fcs_key = '_'.join(fcs_parts[:2]) if len(fcs_parts) >= 2 else fcs_parts[0]
            base = fcs_parts[0]
            date_str = base[:8]
            year = date_str[:4]
            month = date_str[4:6]

            matched_pdfs = [
                pdf for pdf in pdf_files if pdf.startswith(fcs_key)
            ]

            records.append({
                'Study': study_name,
                'Disease/Condition': condition,
                'Subfolder': subfolder,
                'FCS File': fcs,
                'Year': year,
                'Month': month,
                'Is there a PDF report?': bool(matched_pdfs),
                '# of PDF reports associated with fcs file': len(matched_pdfs),
                'PDF reports names:': ', '.join(matched_pdfs)
                
            })

    df = pd.DataFrame(records)
    return df

"""Function for parsing one study folder that does not have any subfolders - a folder that you downloaded as zip from red cap and
reformatted to follow the same formatting pattern as folders from OneDrive"""

def parse_fcs_folder_no_subfolders(folder):
    study_condition_map = {
        'PASC': 'long COVID (Post-acute sequelae of SARS-CoV-2 infection)',
        'Abbvie': 'scleroderma',
        'Duke_ozone': 'control samples, healthy volunteers',
        'WashU_BAL': 'pneumonia',
        'SCRIPT': 'pneumonia'
    } # add more studies if needed
  
    records = []


    study_name = os.path.basename(folder)
    condition = study_condition_map.get(study_name, 'Unknown')

    fcs_files = [f for f in os.listdir(folder) if f.endswith('.fcs')]
    pdf_files = [f for f in os.listdir(folder) if f.endswith('.pdf')]

    for fcs in fcs_files:
        fcs_parts = fcs.split('_')
        fcs_key = '_'.join(fcs_parts[:2]) if len(fcs_parts) >= 2 else fcs_parts[0]
        base = fcs_parts[0]
        date_str = base[:8]
        year = date_str[:4]
        month = date_str[4:6]

        matched_pdfs = [
            pdf for pdf in pdf_files if pdf.startswith(fcs_key)
        ]

        records.append({
            'Study': study_name,
            'Disease/Condition': condition,
            'Subfolder': f"{os.path.join(os.getcwd(),folder)}",
            'FCS File': fcs,
            'Year': year,
            'Month': month,
            'Is there a PDF report?': bool(matched_pdfs),
            '# of PDF reports associated with fcs file': len(matched_pdfs),
            'PDF reports names:': ', '.join(matched_pdfs)

        })

    df = pd.DataFrame(records)
    return df
