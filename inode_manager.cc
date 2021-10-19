#include "inode_manager.h"
#include <ctime>

#define DEBUG 0
#define debug_log(...) do{ \
    if(DEBUG){ \
      printf("[INFO]File: %s\t line: %d: ", __FILE__, __LINE__); \
      printf(__VA_ARGS__); \
      fflush(stdout); \
    } }while(0);

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void disk::read_block(blockid_t id, char *buf)
{
  memcpy(buf, blocks[id], BLOCK_SIZE);
}

void disk::write_block(blockid_t id, const char *buf)
{
  memcpy(blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
  blockid_t start = IBLOCK(INODE_NUM, sb.nblocks) + 1;
  for(blockid_t i = start; i < BLOCK_NUM; i++){
    if(using_blocks.count(i) == 0){
      using_blocks[i] = 1;
      return i;
    }
  }
}

void block_manager::free_block(uint32_t id)
{
  /* 
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  using_blocks.erase(id);
  return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

}

void block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
  
  /* 
   * WindowsXp
   * since we don't have bitmap for inode table, 
   * we have to iterate through the inode table to find freed inodes 
   */
  char buf[BLOCK_SIZE];
  inode_t *ino;
  for(uint32_t inum = 1; inum < INODE_NUM; inum++){
    bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
    ino = (inode_t*)buf + inum%IPB;
    if(ino->type == 0){
      ino->type = type;
      ino->size = 0;
      bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
      return inum;
    }
  }
}

void inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
  char buf[BLOCK_SIZE];
  inode_t *ino_disk, *ino;
  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (inode_t*)buf + inum%IPB;
  if(ino_disk->type != 0){
    ino_disk->type = 0;
    bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
  }
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
inode_t* inode_manager::get_inode(uint32_t inum)
{
  inode_t *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);

  if (inum < 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (inode_t*)buf + inum%IPB;
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (inode_t*)malloc(sizeof(inode_t));
  *ino = *ino_disk;

  return ino;
}

void inode_manager::put_inode(uint32_t inum, inode_t *ino)
{
  char buf[BLOCK_SIZE];
  inode_t *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf); //先要read，因为一个block中不止一个inode
  ino_disk = (inode_t*)buf + inum%IPB;
  *ino_disk = *ino; // 将inode放入这个块中
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))
#define MAX(a,b) (((int)(a))<((int)(b)) ? ((int)(b)) : ((int)(a)))

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_out
   */
  inode_t* ino = get_inode(inum);
  unsigned int file_size = ino->size;
  *size = file_size;
  if(file_size == 0) {
    printf("read an empty file\n");
    return;
  }

  unsigned int block_num = (file_size -1)/BLOCK_SIZE + 1;
  char* buf_p = *buf_out = (char*)malloc(block_num * BLOCK_SIZE);
  debug_log("read file inode: %d\tsize: %d\tblock size: %d\n", inum, file_size, block_num);

  for(unsigned int i = 0; i < block_num; i++, buf_p += BLOCK_SIZE){
    read_nth_block(ino, i, buf_p);
  }
  free(ino);
}


/* alloc/free blocks if needed */
void inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */
  inode_t* ino = get_inode(inum);
  std::time_t t = std::time(0);
  ino->atime = t;
  ino->ctime = t;
  ino->mtime = t;

  std::string content;

  unsigned int original_size = ino->size;
  ino->size = size;
  debug_log("write file inode: %d\t size: %d\toriginal size: %d\n", inum, size, original_size);
  blockid_t* block_array = ino->blocks;
  unsigned int block_num = size == 0 ? 0 : ((size - 1)/BLOCK_SIZE + 1);
  unsigned int original_block_num = original_size == 0 ? 0 : ((original_size - 1)/BLOCK_SIZE + 1);

  if(size < original_size){
    for(unsigned int i = block_num; i < original_block_num; i++){
      free_nth_block(ino, i);
    }
    if(original_block_num > NDIRECT && block_num <= NDIRECT){
      bm->free_block(ino->blocks[NDIRECT]);
    }
  } else {
    for(unsigned int i = original_block_num; i < block_num; i++){
      alloc_nth_block(ino, i, content, false);
    }
  }

  if(size != 0){
    //write until last block
    for (unsigned i = 0; i + 1 < block_num; i++, buf += BLOCK_SIZE) {
      content.assign(buf, BLOCK_SIZE);
      write_nth_block(ino, i, content);
    }

    //write last block, resize to BLOCK_SIZE to avoid undefined behavior when memcpy
    uint32_t remain_bytes = size - (block_num - 1) * BLOCK_SIZE;
    content.assign(buf, remain_bytes);
    content.resize(BLOCK_SIZE);
    write_nth_block(ino, block_num - 1, content);
  }

  put_inode(inum, ino);
  free(ino);
}

void inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  inode_t* ino = get_inode(inum);
  if(ino == NULL){
    printf("[ERROR]invalid inode\n");
    return;
  }
  a.type = ino->type;
  a.atime = ino->atime;
  a.mtime = ino->mtime;
  a.ctime = ino->ctime;
  a.size = ino->size;
  free(ino);
}

void inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
  inode_t* ino = get_inode(inum);
  if(ino == NULL){
    printf("invalid inode number\n");
    return;
  }
  unsigned int size = ino->size;
  unsigned int block_num = size == 0 ? 0 : ((size - 1)/BLOCK_SIZE + 1);
  debug_log("remove file inode: %d\tsize: %d\tblock size: %d\n", inum, size, block_num);

  for(unsigned int i = 0; i < block_num; i++){
    free_nth_block(ino, i);
  }
  if(block_num >= NDIRECT) bm->free_block(ino->blocks[NDIRECT]);
  //free inode
  free_inode(inum);
  free(ino);
}

blockid_t inode_manager::get_nth_blockid(struct inode *ino, uint32_t nth){
  if(nth < NDIRECT)
    return ino->blocks[nth];

  blockid_t indirect_blockid = ino->blocks[NDIRECT];
  blockid_t indirect_block[NINDIRECT];
  bm->read_block(indirect_blockid, (char*)indirect_block);

  return indirect_block[nth - NDIRECT];
}

void inode_manager::free_nth_block(struct inode *ino, uint32_t nth){
  blockid_t id = get_nth_blockid(ino, nth);
  bm->free_block(id);
}

void inode_manager::write_nth_block(struct inode *ino, uint32_t nth, std::string &buf){
  blockid_t blockid = get_nth_blockid(ino, nth);
  assert(buf.size() == BLOCK_SIZE);
  bm->write_block(blockid, buf.data());
}

void inode_manager::alloc_nth_block(struct inode *ino, uint32_t nth, std::string &buf, bool to_write){
  blockid_t blockid = bm->alloc_block();
  if (to_write)
    bm->write_block(blockid, buf.data());
  if (nth < NDIRECT)
    ino->blocks[nth] = blockid;
  else {
    blockid_t indirect_blockid;
    blockid_t indirect_block[NINDIRECT] = {0};
    if(nth == NDIRECT){
      indirect_blockid = bm->alloc_block();
      ino->blocks[NDIRECT] = indirect_blockid;
    } else {
      indirect_blockid = ino->blocks[NDIRECT];
      bm->read_block(indirect_blockid, (char*)indirect_block);
    }
    indirect_block[nth - NDIRECT] = blockid;
    bm->write_block(indirect_blockid, (char*)indirect_block);
  }
}

void inode_manager::read_nth_block(struct inode *ino, uint32_t nth, char* buf){
  blockid_t blockid = get_nth_blockid(ino, nth);
  bm->read_block(blockid, buf);
}