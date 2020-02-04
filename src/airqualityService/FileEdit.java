package airqualityService;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;

public class FileEdit {

	/**
	 * 파일내의 특정 라인에 아규먼트로 입력하는 문자열을 삽입
	 * 
	 * @param inFile
	 *            : 입력 파일명
	 * @param lineno
	 *            : 문자열을 삽일할 라인
	 * @param lineToBeInserted
	 *            : 삽입될 문자열
	 * @throws Exception
	 */
	public void editStringInFile(File inFile) throws Exception {

		try {
			// 파일에서 읽은 한라인을 저장하는 임시변수
			String resultCol = "";

			// 임시파일을 만듭니다.
			File outFile = new File("$$$$$$$$.tmp");

			// 아규먼트로 받은 입력 파일
			FileInputStream fis = new FileInputStream(inFile);
			BufferedReader in = new BufferedReader(new InputStreamReader(fis));

			// output 파일
			FileOutputStream fos = new FileOutputStream(outFile);
			PrintWriter out = new PrintWriter(fos);

			// 파일 내용을 한라인씩 읽어 마지막 두 글자인 구분자를 삭제
			while ((resultCol = in.readLine()) != null) {

				resultCol = resultCol.substring(0, resultCol.length() - 2);

				out.write(resultCol + "\r\n");
				

			}
			
			out.flush();
			out.close();
			in.close();

			inFile.delete();

			// 임시파일을 원래 파일명으로 변경
			outFile.renameTo(inFile);

			System.out.println("CHANGE OK~~~");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
