#include "inode_manager.h"

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
   * since we don't have bitmap for inode table, 
   * we have to iterate through the inode table to find freed inodes
   */
  char buf[BLOCK_SIZE];
  inode_t *ino;
  uint32_t inum = 1;
  for(; inum < INODE_NUM; inum++){
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
  blockid_t* block_array = ino->blocks;
  unsigned int block_size = file_size/BLOCK_SIZE + 1;
  char* buf_p = *buf_out = (char*)malloc(block_size * BLOCK_SIZE);

  unsigned int direct_block_size = MIN(block_size, NDIRECT);
  for(blockid_t i = 0; i < direct_block_size; i++, buf_p += BLOCK_SIZE){
    bm->read_block(block_array[i], buf_p);
  }
  if(direct_block_size > NDIRECT){
    unsigned int undirect_block_size = block_size - NDIRECT;
    blockid_t inderect_block[BLOCK_SIZE];
    bm->read_block(block_array[NDIRECT], (char*)inderect_block);
    for(blockid_t i = 0; i < undirect_block_size; i++, buf_p += BLOCK_SIZE){
      bm->read_block(inderect_block[i], buf_p);
    }
  }
  delete ino;
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
  unsigned int original_size = ino->size;
  unsigned int file_size = MIN(size, original_size);
  ino->size = file_size;
  if(file_size == size){ //smaller
    //free the spare blocks
    unsigned int block_size = file_size/BLOCK_SIZE + 1;
    unsigned int original_block_size = original_size/BLOCK_SIZE + 1;
    unsigned int direct_block_size = MIN(block_size, NDIRECT);
    for(blockid_t i = 0; i < direct_block_size; i++, buf += BLOCK_SIZE){
      bm->write_block(ino->blocks[i], buf);
    }
    
  } else { //bigger
    //add blocks into inode::blocks
    for(; file_size > 0; file_size -= BLOCK_SIZE){
      blockid_t new_block = bm->alloc_block();
    }
  }
  delete ino;
}

void inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  inode_t* ino = get_inode(inum);
  a.type = ino->type;
  a.atime = ino->atime;
  a.mtime = ino->mtime;
  a.ctime = ino->ctime;
  a.size = ino->size;
  delete ino;
}

void inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
  
  return;
}