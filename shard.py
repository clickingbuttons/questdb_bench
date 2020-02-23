import sys
import os
import shutil

db_name = 'trades_test'
split_path = '/mnt/ssd1/db/' + db_name
ssds = ['/mnt/ssd' + str(n + 1) for n in range(8)]
dirs = [f for f in os.listdir(split_path) if os.path.isdir(os.path.join(split_path, f)) and not os.path.islink(os.path.join(split_path, f))]

for i, d in enumerate(sorted(dirs)):
    from_path = os.path.join(split_path, d)
    to_path = os.path.join(ssds[i % len(ssds)], 'db_shard', db_name, d)
    sym_path = os.path.join(split_path, d)
    print(i, from_path, '->', to_path)
    shutil.move(from_path, to_path)
    os.symlink(to_path, sym_path)
    # data_files = [f for f in os.listdir(from_path)] 
    # print(data_files)
    # print(sum(os.path.getsize(os.path.join(split_path, d, f)) for f in data_files)) 
