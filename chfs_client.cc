// chfs client.  implements FS operations using extent and lock server
#include "chfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#define DEBUG 0
#define debug_log(...) do{ \
    if(DEBUG){ \
      printf("[INFO]File: %s\t line: %d: ", __FILE__, __LINE__); \
      printf(__VA_ARGS__); \
      fflush(stdout); \
    } }while(0);

chfs_client::chfs_client()
{
    ec = new extent_client();

}

chfs_client::chfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client();
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

chfs_client::inum chfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string chfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

size_t chfs_client::string_size(char* p){
    return strlen(p) + 1;
}

bool chfs_client::isfile(inum inum)
{
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    printf("isfile: %lld is a dir\n", inum);
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

bool chfs_client::isdir(inum inum)
{
    // Oops! is this still correct when you implement symlink?
    return ! isfile(inum);
}

int chfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int chfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int chfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    std::string buf;
    if(ec->get(ino, buf) != extent_protocol::OK){
        printf("file not exist\n");
        r = IOERR;
        goto release;
    }

    buf.resize(size);

    if(ec->put(ino, buf) != OK){
        printf("write back error\n");
        r = IOERR;
        goto release;
    }

release:
    return r;
}

int chfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    debug_log("create file %s in %lld\n", name, parent);
    std::string buf;
    if(ec->get(parent, buf) != OK){
        printf("parent directory not exist\n");
        r = IOERR;
        goto release;
    }
    inum new_file;
    if(ec->create(extent_protocol::T_FILE, new_file) != OK){
        printf("create new file error\n");
        r = IOERR;
        goto release;
    }
    ino_out = new_file;
    debug_log("new file's inode is %lld\n", new_file);
    buf.append(name);
    buf.push_back('\0');
    buf.append(filename(new_file));
    buf.push_back('\0');
    debug_log("new dirent.name is %s\tdirent.num is %s", name, filename(new_file).data());
    
    if(ec->put(parent, buf) != OK){
        printf("[ERROR]update parent diretory error\n");
        r = IOERR;
        goto release;
    }
release:
    return r;
}

int
chfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    std::string buf;
    if(ec->get(parent, buf) != OK){
        printf("parent directory not exist\n");
        r = IOERR;
        goto release;
    }
    inum new_dir;
    if(ec->create(extent_protocol::T_DIR, new_dir) != OK){
        printf("create new directory error\n");
        r = IOERR;
        goto release;
    }
    ino_out = new_dir;
    buf.append(name);
    buf.push_back('\0');
    buf.append(filename(new_dir));
    buf.push_back('\0');
    if(ec->put(parent, buf) != OK){
        printf("update parent diretory error\n");
        r = IOERR;
        goto release;
    }
release:
    return r;
}

int
chfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    /*
     * format: name\0inum\0name\0inum\0
     */
    debug_log("look for file %s in parent %lld\n", name, parent);
    std::string buf;
    const char* dirent_p;
    const char* end_p;
    if(ec->get(parent, buf) != OK){
        printf("parent directory not exist\n");
        r = IOERR;
        goto release;
    }

    found = false;
    dirent_p = buf.data();
    end_p = dirent_p + buf.size();
    while(dirent_p < end_p){
        std::string dirent_name(dirent_p);
        dirent_p += dirent_name.length() + 1;
        std::string dirent_inmu(dirent_p);
        dirent_p += dirent_inmu.length() + 1;
        debug_log("dirent.name is %s\tdirent.inum is %s\n", dirent_name.data(), dirent_inmu.data());
        if(dirent_name.compare(name) == 0){
            found = true;
            ino_out = n2i(dirent_inmu);
            break;
        }
    }

release:
    return r;
}

int
chfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */

    debug_log("read directory %lld\n", dir);
    std::string buf;
    const char* dirent_p;
    const char* end_p;
    if(ec->get(dir, buf) != OK){
        printf("directory not exist\n");
        r = IOERR;
        goto release;
    }

    dirent_p = buf.data();
    end_p = dirent_p + buf.size();

    while(dirent_p < end_p){
        struct dirent new_dirent;
        new_dirent.name = dirent_p;
        dirent_p += new_dirent.name.length() + 1;
        new_dirent.inum = n2i(dirent_p);
        dirent_p += strlen(dirent_p) + 1;
        list.push_back(new_dirent);
    }
release:
    return r;
}

int
chfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;
    debug_log("read file %lld\n", ino);
    /*
     * your code goes here.
     * note: read using ec->get().
     */

    return r;
}

int
chfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;
    debug_log("write file %lld\n", ino);
    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */

    return r;
}

int chfs_client::unlink(inum parent,const char *name)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */

    return r;
}

