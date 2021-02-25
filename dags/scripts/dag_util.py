from pathlib import Path

def construct_file_path(file_directory, file_name):
    if file_directory:
        if isinstance(file_directory, str):
            return '/'.join([file_directory.strip('/'), file_name.strip('/')])
        elif isinstance(file_directory, Path):
            return str(file_directory.joinpath(file_name).resolve())
        else:
            raise ValueError('File directory must be a string or pathlib.Path')
    
    return file_name
