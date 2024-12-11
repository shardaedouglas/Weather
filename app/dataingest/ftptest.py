from ftplib import FTP

# FTP server credentials
ftp_host = "elandwg.duckdns.org"
ftp_user = "testuser123"
ftp_pass = "nothingwillgowrong"

def upload_file_to_ftp(file_path):
    
    # Connect to the FTP server
    ftp = FTP(ftp_host)
    ftp.login(user=ftp_user, passwd=ftp_pass)
    
    try:
        # Open the file in binary mode
        with open(file_path, "rb") as file:
            # Extract the filename from the file path
            file_name = file_path.split("/")[-1]
            
            # Upload the file to the FTP server
            ftp.storbinary(f"STOR {file_name}", file)
        
        print(f"File '{file_name}' uploaded successfully to {ftp_host}")
        
    except Exception as e:
        print(f"An error occurred: {e}")
    
    finally:
        # Close the FTP connection
        ftp.quit()


def list_files_on_ftp():
    
    # Connect to the FTP server
    ftp = FTP(ftp_host)
    ftp.login(user=ftp_user, passwd=ftp_pass)
    
    try:
        # List files in the current directory
        files = ftp.nlst()
        print("Files on FTP server:")
        for file in files:
            print(file)
    
    except Exception as e:
        print(f"An error occurred: {e}")
    
    finally:
        # Close the FTP connection
        ftp.quit()

from ftplib import FTP

def delete_file_from_ftp(file_name):
    
    # Connect to the FTP server
    ftp = FTP(ftp_host)
    ftp.login(user=ftp_user, passwd=ftp_pass)
    
    try:
        # Delete the specified file
        ftp.delete(file_name)
        print(f"File '{file_name}' deleted successfully from {ftp_host}")
        
    except Exception as e:
        print(f"An error occurred: {e}")
    
    finally:
        # Close the FTP connection
        ftp.quit()

# Usage
delete_file_from_ftp("test.txt")


# Usage
list_files_on_ftp()

# Usage
upload_file_to_ftp("test.txt")
list_files_on_ftp()