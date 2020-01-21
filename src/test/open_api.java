package test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Set;
import java.util.Date;
import java.util.Iterator;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class open_api {

	//대기질 정보 조회 - 조사속성조회
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		
		System.out.println("오픈api 시작"); 
		
		//step 0.파일의 첫 행 작성
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		Date thisDate = new Date();
		String strDate = format.format(thisDate);
		
		File file = new File("test1_" + strDate + ".txt");
		
		try{
			PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(file,true)));
			pw.write("ivstgGb");
			pw.write("|^");
			pw.write("ivstgSpotNm");
			pw.write("|^");
			pw.write("adres");
			pw.write("|^");
			pw.write("coVal");
			pw.write("|^");
			pw.write("ivstgEndde");
			pw.write("|^");
			pw.write("pm10Val");
			pw.write("|^");
			pw.write("pbVal");
			pw.write("|^");
			pw.write("no2Val");
			pw.write("|^");
			pw.write("o3Val");
			pw.write("|^");
			pw.write("so2Val");
			pw.write("|^");
			pw.write("ivstgBgnde");
			pw.write("|^");
			pw.write("ivstgOdr");
			pw.write("|^");
			pw.write("envrnGrad");
			pw.write("|^");
			//두번째 배열은 컬럼값이 다름
			pw.write("umenoNo224hr");
			pw.write("|^");
			pw.write("umenoCo1hr");
			pw.write("|^");
			pw.write("umenoNo21hr");
			pw.write("|^");
			pw.write("umenoSo21hr");
			pw.write("|^");
			pw.write("umenoSo2Year");
			pw.write("|^");
			pw.write("umenoPm10Year");
			pw.write("|^");
			pw.write("umenoCo8hr");
			pw.write("|^");
			pw.write("umenoPm1024hr");
			pw.write("|^");
			pw.write("umenoSo224hr");
			pw.write("|^");
			pw.write("umenoNo2Year");
			pw.write("|^");
			pw.write("gmenoNo21hr");
			pw.write("|^");
			pw.write("gmenoPm10Year");
			pw.write("|^");
			pw.write("gmenoNo224hr");
			pw.write("|^");
			pw.write("gmenoNo2Year");

			pw.println();
			pw.flush();
			pw.close();
			
		}catch (IOException e){
			e.printStackTrace();
		}

		//step 2.오픈 api를 이용해 json 형태로 받아 옴
		BufferedReader br = null;
		String json = "";
        try{
        	
        	String service_key = "WohJ4yqANNo0tHxkIX0%2BIB6hrluOlUc66TXvVGGarW%2Bs1wKzTEG2%2Bx8u08A%2BsyaE5%2B0HZ%2F9IFhHt91nYIf5VEw%3D%3D";
        	String mgtNo = "DG2007B002";
            String urlstr = "http://apis.data.go.kr/B090026/AirqualityService/getIvstg?type=json&mgtNo="+mgtNo +"&serviceKey="+service_key;
            
            URL url = new URL(urlstr);
            HttpURLConnection urlconnection = (HttpURLConnection) url.openConnection();
            urlconnection.setRequestMethod("GET");
            br = new BufferedReader(new InputStreamReader(urlconnection.getInputStream(),"UTF-8"));
           
            String line;
            while((line = br.readLine()) != null) {
            	json = json + line + "\n";
            }
            
            //테스트 출력
            //System.out.println(json);
            
        }catch(Exception e){
            System.out.println(e.getMessage());
        }
        
        //step 3.필요에 맞게 파싱
    
        JSONParser parser = new JSONParser();
		JSONObject obj = (JSONObject)parser.parse(json);
		JSONObject response = (JSONObject)obj.get("response");
		
		//response는 결과값 코드와 메시지를 가지는 header와 데이터 부분인 body로 구분
		JSONObject body = (JSONObject)response.get("body");
		
		JSONArray ivstgGbs = (JSONArray)body.get("ivstgGbs");
		
		//System.out.println(ivstgGbs);
		
		for(int i=0; i<ivstgGbs.size(); i++){
			JSONObject ivstgGb_Json = (JSONObject)ivstgGbs.get(i);
			
			String ivstgGb_str = ivstgGb_Json.get("ivstgGb").toString();
					
			//공통으로 갖는 조사구분값
			//System.out.println("ivstgGb : " + ivstgGb_str);
			
			JSONArray ivstgs = (JSONArray)ivstgGb_Json.get("ivstgs");
			
			for(int r=0; r<ivstgs.size(); r++){
				
				//System.out.println("r :"+ r );
				
				JSONObject ivstg = (JSONObject)ivstgs.get(r);
				
				String ivstg_addr_str = "";
				String ivstg_ivstgSpotNm_str = "";
				
				if(ivstg.get("adres") != null){
					ivstg_addr_str = ivstg.get("adres").toString();
				}else{
					ivstg_addr_str = "";
				}
				
				if(ivstg.get("ivstgSpotNm") != null){
					ivstg_ivstgSpotNm_str = ivstg.get("ivstgSpotNm").toString();
				}else{
					ivstg_ivstgSpotNm_str = "";
				}
				
				//System.out.println("ivstgSpotNm : " + ivstg_ivstgSpotNm_str);
				//System.out.println("adres : " + ivstg_addr_str);
				
				//System.out.println(ivstg);
				
				JSONArray odrs = (JSONArray)ivstg.get("odrs");
				
				for(int f=0; f<odrs.size(); f++){
					
					//System.out.println("ivstgGb : " + ivstgGb_str);
					//System.out.println("ivstgSpotNm : " + ivstg_ivstgSpotNm_str);
					//System.out.println("adres : " + ivstg_addr_str);
					
					JSONObject odr = (JSONObject)odrs.get(f);
					
					//System.out.println(odr);
					
					Set<String> key = odr.keySet();
					
					Iterator<String> iter = key.iterator();
					
					//3개는 공통적인 바깥 배열의 값이므로 Iterator로 받아 올 수 없다..
					//String ivstgGb = "";
					//String ivstgSpotNm = "";
					//String adres = "";
					String coVal = "";
					String ivstgEndde = "";
					String pm10Val = "";
					String pbVal = "";
					String no2Val = "";
					String o3Val = "";
					String so2Val = "";
					String ivstgBgnde = "";
					String ivstgOdr = "";
					String envrnGrad = "";
					String umenoNo224hr = "";
					String umenoCo1hr = "";
					String umenoNo21hr = "";
					String umenoSo21hr = "";
					String umenoSo2Year = "";
					String umenoPm10Year = "";
					String umenoCo8hr = "";
					String umenoPm1024hr = "";
					String umenoSo224hr = "";
					String umenoNo2Year = "";
					String gmenoNo21hr = "";
					String gmenoPm10Year = "";
					String gmenoNo224hr = "";
					String gmenoNo2Year = "";
					
					while(iter.hasNext()){
						String keyname = iter.next();
						
						//System.out.println("key : " + keyname + " type : " + odr.get(keyname).getClass() + "");
						//System.out.println("value : " + odr.get(keyname).toString());
						
						System.out.println(keyname + ":" + odr.get(keyname).toString());
						
						//3개는 공통적인 바깥 배열의 값이므로 Iterator로 받아 올 수 없다..
						/*if(keyname.equals("ivstgGb")){
							ivstgGb = odr.get(keyname).toString();
						}
						if(keyname.equals("ivstgSpotNm")){
							ivstgSpotNm = odr.get(keyname).toString();
						}
						if(keyname.equals("adres")){
							adres = odr.get(keyname).toString();
						}*/
						
						if(keyname.equals("coVal")){
							coVal = odr.get(keyname).toString();
						}
						if(keyname.equals("ivstgEndde")){
							ivstgEndde = odr.get(keyname).toString();
						}
						if(keyname.equals("pm10Val")){
							pm10Val = odr.get(keyname).toString();
						}
						if(keyname.equals("pbVal")){
							pbVal = odr.get(keyname).toString();
						}
						if(keyname.equals("no2Val")){
							no2Val = odr.get(keyname).toString();
						}
						if(keyname.equals("o3Val")){
							o3Val = odr.get(keyname).toString();
						}
						if(keyname.equals("so2Val")){
							so2Val = odr.get(keyname).toString();
						}
						if(keyname.equals("ivstgBgnde")){
							ivstgBgnde = odr.get(keyname).toString();
						}
						if(keyname.equals("ivstgOdr")){
							ivstgOdr = odr.get(keyname).toString();
						}
						if(keyname.equals("envrnGrad")){
							envrnGrad = odr.get(keyname).toString();
						}
						if(keyname.equals("umenoNo224hr")){
							umenoNo224hr = odr.get(keyname).toString();
						}
						if(keyname.equals("umenoCo1hr")){
							umenoCo1hr = odr.get(keyname).toString();
						}
						if(keyname.equals("umenoNo21hr")){
							umenoNo21hr = odr.get(keyname).toString();
						}
						if(keyname.equals("umenoSo21hr")){
							umenoSo21hr = odr.get(keyname).toString();
						}
						if(keyname.equals("umenoSo2Year")){
							umenoSo2Year = odr.get(keyname).toString();
						}
						if(keyname.equals("umenoPm10Year")){
							umenoPm10Year = odr.get(keyname).toString();
						}
						if(keyname.equals("umenoCo8hr")){
							umenoCo8hr = odr.get(keyname).toString();
						}
						if(keyname.equals("umenoPm1024hr")){
							umenoPm1024hr = odr.get(keyname).toString();
						}
						if(keyname.equals("umenoSo224hr")){
							umenoSo224hr = odr.get(keyname).toString();
						}
						if(keyname.equals("umenoNo2Year")){
							umenoNo2Year = odr.get(keyname).toString();
						}
						if(keyname.equals("gmenoNo21hr")){
							gmenoNo21hr = odr.get(keyname).toString();
						}
						if(keyname.equals("gmenoPm10Year")){
							gmenoPm10Year = odr.get(keyname).toString();
						}
						if(keyname.equals("gmenoNo224hr")){
							gmenoNo224hr = odr.get(keyname).toString();
						}
						if(keyname.equals("gmenoNo2Year")){
							gmenoNo2Year = odr.get(keyname).toString();
						}
					}
					
					
					//step 4. 파일에 쓰기
					
					try{
						PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(file,true)));
						pw.write(ivstgGb_str);
						pw.write("|^");
						pw.write(ivstg_ivstgSpotNm_str);
						pw.write("|^");
						pw.write(ivstg_addr_str);
						pw.write("|^");
						pw.write(coVal);
						pw.write("|^");
						pw.write(ivstgEndde);
						pw.write("|^");
						pw.write(pm10Val);
						pw.write("|^");
						pw.write(pbVal);
						pw.write("|^");
						pw.write(no2Val);
						pw.write("|^");
						pw.write(o3Val);
						pw.write("|^");
						pw.write(so2Val);
						pw.write("|^");
						pw.write(ivstgBgnde);
						pw.write("|^");
						pw.write(ivstgOdr);
						pw.write("|^");
						pw.write(envrnGrad);
						pw.write("|^");
						//두번째 배열은 컬럼값이 다름
						pw.write(umenoNo224hr);
						pw.write("|^");
						pw.write(umenoCo1hr);
						pw.write("|^");
						pw.write(umenoNo21hr);
						pw.write("|^");
						pw.write(umenoSo21hr);
						pw.write("|^");
						pw.write(umenoSo2Year);
						pw.write("|^");
						pw.write(umenoPm10Year);
						pw.write("|^");
						pw.write(umenoCo8hr);
						pw.write("|^");
						pw.write(umenoPm1024hr);
						pw.write("|^");
						pw.write(umenoSo224hr);
						pw.write("|^");
						pw.write(umenoNo2Year);
						pw.write("|^");
						pw.write(gmenoNo21hr);
						pw.write("|^");
						pw.write(gmenoPm10Year);
						pw.write("|^");
						pw.write(gmenoNo224hr);
						pw.write("|^");
						pw.write(gmenoNo2Year);
						pw.write("|^");

						pw.println();
						pw.flush();
						pw.close();
						
					}catch (IOException e){
						e.printStackTrace();
					}
	
				}
			}
			
			System.out.println("next");
		}
			System.out.println("오픈api 끝");
	}

}
