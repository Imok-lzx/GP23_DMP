/*
 * 深度优先搜索  dfs   Depth-first search
 *  个人理解：dfs 即深搜 返回上一节点的其他情况 其实本质上就是用的
 *  递归的性质  因为递归最底层执行完会去返回上一层 这个时候 若有别的节点 会去继续执行
 *  因为是在循环里  若没有 则会继续返回到再上一层 达到了 找其他节点的目的 又因为递归
 *  单一执行 会形成深度优先
 */
//深度优先搜素解数独游戏
public class Demo29 {
	public static void main(String[] args) {
		char [] [] a  = {{'0','0','5','3','0','0','0','0','0'},
						{'8','0','0','0','0','0','0','2','0'},
						{'0','7','0','0','1','0','5','0','0'},
						{'4','0','0','0','0','5','3','0','0'},
						{'0','1','0','0','7','0','0','0','6'},
						{'0','0','3','2','0','0','0','8','0'},
						{'0','6','0','5','0','0','0','0','9'},
						{'0','0','4','0','0','0','0','3','0'},
						{'0','0','0','0','0','9','7','0','0'}
						};
		int x  = 0 ;
		int y = 0 ;
		dfsSuduke(a,x,y);
	}

	private static void dfsSuduke(char[][] a, int x, int y) {
		// TODO Auto-generated method stub
		if(x==9)
		{
			for(int i = 0 ; i < 9; i++) {
				for(int j = 0 ; j < 9;j++) {
					System.out.print(a[i][j]+" ");
				}
				System.out.println();
			}
			System.exit(0);
		}
		if(a[x][y] =='0') {
			for(int i = 1 ;i < 10;i++) {
				if(check(a,x,y,i)) {
					a[x][y] = (char)('0'+i);
					dfsSuduke(a,x+(y+1)/9,(y+1)%9);
				}
			}
			a[x][y] = '0';
		}
		else {
			dfsSuduke(a,x+(y+1)/9,(y+1)%9);
		}
	}

	private static boolean check(char[][] a, int x, int y, int k) {
		for(int i =0 ; i < 9;i++)
		{
			if(a[i][y] =='0'+k) return false;
			if(a[x][i]=='0'+k) return false;
		}
		
		for(int i = (x/3)*3 ; i < (x/3+1)*3;i++) {
			for(int j = (y/3)*3; j <(y/3+1)*3;j++) {
				if(a[i][j]=='0'+k) return false;
			}
		}
		return true;
	}
	

	
	
}
