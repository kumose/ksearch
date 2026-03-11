#include <ksearch/common/file_system.h>

namespace ksearch {
    DEFINE_int32(file_buffer_size, 1, "read file buf size (MBytes)");
    DEFINE_int32(file_block_size, 100, "split file to block to handle(MBytes)");

    const char *AFS_CLIENT_CONF_PATH = "./conf/client.conf";

    // PosixFileReader
    int64_t PosixFileReader::read(size_t pos, char *buf, size_t buf_size) {
        if (_error) {
            DB_WARNING("file: %s read failed", _path.c_str());
            return -1;
        }
        int64_t size = _f.Read(pos, buf, buf_size);
        if (size < 0) {
            DB_WARNING("file: %s read failed", _path.c_str());
        }
        DB_DEBUG("read file :%s, pos: %ld, read_size: %ld", _path.c_str(), pos, size);
        return size;
    }

    // PosixFileSystem
    std::shared_ptr<FileReader> PosixFileSystem::open_reader(const std::string &path) {
        std::shared_ptr<FileReader> file_reader(new PosixFileReader(path));
        return file_reader;
    }

    int PosixFileSystem::close_reader(std::shared_ptr<FileReader> file_reader) {
        return 0;
    }

    ::arrow::Result<std::shared_ptr<::arrow::io::RandomAccessFile> >
    PosixFileSystem::open_arrow_file(const std::string &path) {
        return ::arrow::io::ReadableFile::Open(path);
    }

    ::arrow::Status PosixFileSystem::close_arrow_reader(
        std::shared_ptr<::arrow::io::RandomAccessFile> arrow_reader) {
        if (arrow_reader != nullptr && !arrow_reader->closed()) {
            return arrow_reader->Close();
        }
        return ::arrow::Status::OK();
    }

    int PosixFileSystem::read_dir(const std::string &path, std::vector<std::string> &direntrys) {
        dir_iter iter(path);
        dir_iter end;
        for (; iter != end; ++iter) {
            std::string child_path = iter->path().c_str();
            std::vector<std::string> split_vec;
            boost::split(split_vec, child_path, boost::is_any_of("/"));
            std::string out_path = split_vec.back();
            direntrys.emplace_back(out_path);
        }
        return 1;
    }

    int PosixFileSystem::read_dir(const std::string &path, std::vector<DirEntry> &direntrys) {
        dir_iter iter(path);
        dir_iter end;
        for (; iter != end; ++iter) {
            std::string child_path = iter->path().c_str();
            FileInfo file_info;
            if (get_file_info(child_path, file_info, nullptr) != 0) {
                return -1;
            }
            std::vector<std::string> split_vec;
            boost::split(split_vec, child_path, boost::is_any_of("/"));
            std::string out_path = split_vec.back();
            DirEntry dir_entry;
            dir_entry.mode = file_info.mode;
            dir_entry.path = out_path;
            direntrys.emplace_back(std::move(dir_entry));
        }
        return 1;
    }

    int PosixFileSystem::get_file_info(const std::string &path, FileInfo &file_info, std::string *err_msg) {
        if (boost::filesystem::is_directory(path)) {
            file_info.mode = FileMode::I_DIR;
        } else if (boost::filesystem::is_symlink(path)) {
            file_info.mode = FileMode::I_LINK;
        } else if (boost::filesystem::is_regular_file(path)) {
            file_info.mode = FileMode::I_FILE;
            file_info.size = boost::filesystem::file_size(path);
        } else {
            DB_WARNING("path:%s is not dir/file/link", path.c_str());
            return -1;
        }
        return 0;
    }


    // ReadDirImpl
    int ReadDirImpl::next_entry(std::string &entry) {
        if (_idx >= _entrys.size() && _read_finish) {
            return 1;
        }
        if (_idx < _entrys.size()) {
            entry = _entrys[_idx++];
            DB_WARNING("readdir: %s, get next entry: %s", _path.c_str(), entry.c_str());
            return 0;
        }
        _idx = 0;
        _entrys.clear();
        int ret = _fs->read_dir(_path, _entrys);
        if (ret < 0) {
            DB_WARNING("readdir: %s failed", _path.c_str());
            return -1;
        } else if (ret == 1) {
            _read_finish = true;
        }
        if (_entrys.empty()) {
            return 1;
        }
        entry = _entrys[_idx++];
        DB_WARNING("readdir: %s, get next entry: %s", _path.c_str(), entry.c_str());
        return 0;
    }

    int ReadDirImpl::get_all_files(FileSystem *fs, const std::string &path, std::vector<std::string> &files) {
        if (fs == nullptr) {
            DB_WARNING("fs is nullptr");
            return -1;
        }
        std::vector<DirEntry> direntrys;
        int ret = fs->read_dir(path, direntrys);
        if (ret < 0) {
            DB_WARNING("Fail to read_dir, path: %s", path.c_str());
            return -1;
        }
        for (const auto &direntry: direntrys) {
            if (direntry.mode == FileMode::I_FILE) {
                files.emplace_back(direntry.path);
            }
        }
        return 0;
    }

    int ReadDirImpl::get_all_dirs(FileSystem *fs, const std::string &path, std::vector<std::string> &dirs) {
        if (fs == nullptr) {
            DB_WARNING("fs is nullptr");
            return -1;
        }
        std::vector<DirEntry> direntrys;
        int ret = fs->read_dir(path, direntrys);
        if (ret < 0) {
            DB_WARNING("Fail to read_dir, path: %s", path.c_str());
            return -1;
        }
        for (const auto &direntry: direntrys) {
            if (direntry.mode == FileMode::I_DIR) {
                dirs.emplace_back(direntry.path);
            }
        }
        return 0;
    }

    std::shared_ptr<FileSystem> create_filesystem(const std::string &cluster_name,
                                                  const std::string &user_name,
                                                  const std::string &password,
                                                  const std::string &conf_file) {
        std::shared_ptr<FileSystem> fs;
        if (cluster_name.find("afs") != std::string::npos) {
            DB_FATAL("doesn't support AfsFileSystem!");
            return nullptr;
        } else {
            fs.reset(new(std::nothrow) PosixFileSystem());
        }
        if (fs == nullptr) {
            DB_FATAL("fs is nullptr");
            return nullptr;
        }
        if (fs->init() != 0) {
            DB_FATAL("filesystem init failed, cluster_name: %s", cluster_name.c_str());
            return nullptr;
        }
        return fs;
    }

    int destroy_filesystem(std::shared_ptr<FileSystem> fs) {
        if (fs != nullptr) {
            int ret = fs->destroy();
            if (ret != 0) {
                DB_WARNING("Fail to destroy filesystem");
                return -1;
            }
        }
        return 0;
    }
} // namespace ksearch