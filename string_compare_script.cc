#include <iostream>
#include <cstring>
using namespace std;
char c1[200000];
char c2[200000];
int main(){
	memset(c1, 0, sizeof(c1));
	memset(c1, 0, sizeof(c2));
	FILE* f1 = fopen("1.txt", "r");
	FILE* f2 = fopen("2.txt", "r");
	fscanf(f1, "%s", c1);
	fscanf(f2, "%s", c2);
    printf("size of c1 is %lu\n", strlen(c1));
	for(int i = 0; i < strlen(c1); i++){
		if(c1[i] != c2[i]){
            std::cout << i << '\t' << c1[i] << " - " << c2[i] << std::endl;
        }
	}
    std::cout << "identical!!\n";
    return 0;
}