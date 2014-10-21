function [points,slopes] = threel(X,p)

optS1x=0;optS1y =0;optS2x = 0;optS2y = 0;
optslope1=0;optslope2=0;optslope3 = 0;

optS1=0;
optS2=0;
         
         %use this one for modeling on all data
        if ~ischar(X) && mod(length(X(1,:)),24)==0
                energy=X(1,:);
                temperature=X(2,:);
                int_temp=round(temperature);
                T=zeros(max(int_temp)-min(int_temp)+1,1);
                L=zeros(max(int_temp)-min(int_temp)+1,1);
                counter=0;
                for j=min(int_temp):max(int_temp)
                    if (length(energy(logical(int_temp==j)))>20)
                        counter = counter+1;
                        T(counter,1)=j;
                        L(counter,1)=prctile(energy(logical(int_temp==j)),p);
                    end
                end
                T=T(1:counter,1);
                L=L(1:counter,1);
                
                min_RMSE=100;
                for s1=10:20
                    if(sum(logical(T==s1))==1)
                        TS1=T(logical(T<=s1));
                        LS1=L(logical(T<=s1));
                        TS1=[ones(size(TS1)) TS1];
                        CS1=(TS1'*TS1) \ (TS1'*LS1);
                        PLS1=CS1(1)+CS1(2).*TS1(:,2);
                        RMSE_S1 = sqrt(sum((PLS1 - LS1) .^ 2)/size(TS1,1));
                        for s2=5:10
                            %TS2=T(logical(T>=s1)); %this and the next line are the original implementation, which is incorrect.
							TS2=T(logical(s1<=T & T <= (s1 + s2))); % this is changed by matt                            

							TS2=TS2(1:s2);
                            LS2=L(logical(T>=s1));
                            LS2=LS2(1:s2);
                            TS2=[ones(size(TS2)) TS2];
                            CS2=(TS2'*TS2) \ (TS2'*LS2);
                            PLS2=CS2(1)+CS2(2).*TS2(:,2);
                            RMSE_S2 = sqrt(sum((PLS2 - LS2) .^ 2)/size(TS2,1));

                            TS3=T(logical(T>=TS2(end)));
                            LS3=L(logical(T>=TS2(end)));
                            TS3=[ones(size(TS3)) TS3];
                            CS3=(TS3'*TS3) \ (TS3'*LS3);
                            PLS3=CS3(1)+CS3(2).*TS3(:,2);
                            RMSE_S3 = sqrt(sum((PLS3 - LS3) .^ 2)/size(TS3,1));
                            
                            RMSE = RMSE_S1 + RMSE_S2 + RMSE_S3;
                            if(RMSE < min_RMSE)
                                min_RMSE = RMSE;
                                optS1=s1;
                                optS2=s2;
                            end
                        end
                    end
                    
                end
                TS1=T(logical(T<=optS1));
                LS1=L(logical(T<=optS1));
                TS1=[ones(size(TS1)) TS1];
                CS1=(TS1'*TS1) \ (TS1'*LS1);
                PLS1=CS1(1)+CS1(2).*TS1(:,2);
                TS2=T(logical(T>=optS1));
                TS2=TS2(1:optS2);
                LS2=L(logical(T>=optS1));
                LS2=LS2(1:optS2);
                TS2=[ones(size(TS2)) TS2];
                CS2=(TS2'*TS2) \ (TS2'*LS2);
                PLS2=CS2(1)+CS2(2).*TS2(:,2);
                if(length(TS2)>0)
                TS3=T(logical(T>=TS2(end)));
                LS3=L(logical(T>=TS2(end)));
                TS3=[ones(size(TS3)) TS3];
                CS3=(TS3'*TS3) \ (TS3'*LS3);
                PLS3=CS3(1)+CS3(2).*TS3(:,2);
                end
                optS2=optS2+optS1-1;
                
                TcpA=optS1;
                LcpA1=CS1(1)+TcpA*CS1(2);
                LcpA2=CS2(1)+TcpA*CS2(2);
                LcpA=(LcpA1+LcpA2)/2;
                
                TcpB=optS2;
                LcpB2=CS2(1)+TcpB*CS2(2);
                LcpB3=CS3(1)+TcpB*CS3(2);
                LcpB=(LcpB2+LcpB3)/2;
                
                searchSpace1=zeros(9,2);
                searchSpace2=zeros(9,2);
                
                deltaT=0.5;
                deltaL1=abs(LcpA1-LcpA2)/4;
                deltaL2=abs(LcpB2-LcpB3)/4;
                
                searchSpace1(1,1)=TcpA-deltaT; searchSpace1(1,2)=LcpA+deltaL1;
                searchSpace1(2,1)=TcpA; searchSpace1(2,2)=LcpA+deltaL1;
                searchSpace1(3,1)=TcpA+deltaT; searchSpace1(3,2)=LcpA+deltaL1;
                searchSpace1(4,1)=TcpA-deltaT; searchSpace1(4,2)=LcpA;
                searchSpace1(5,1)=TcpA; searchSpace1(5,2)=LcpA;
                searchSpace1(6,1)=TcpA+deltaT; searchSpace1(6,2)=LcpA;
                searchSpace1(7,1)=TcpA-deltaT; searchSpace1(7,2)=LcpA-deltaL1;
                searchSpace1(8,1)=TcpA; searchSpace1(8,2)=LcpA-deltaL1;
                searchSpace1(9,1)=TcpA+deltaT; searchSpace1(9,2)=LcpA-deltaL1;
                
                searchSpace2(1,1)=TcpB-deltaT; searchSpace2(1,2)=LcpB+deltaL1;
                searchSpace2(2,1)=TcpB; searchSpace2(2,2)=LcpB+deltaL1;
                searchSpace2(3,1)=TcpB+deltaT; searchSpace2(3,2)=LcpB+deltaL1;
                searchSpace2(4,1)=TcpB-deltaT; searchSpace2(4,2)=LcpB;
                searchSpace2(5,1)=TcpB; searchSpace2(5,2)=LcpB;
                searchSpace2(6,1)=TcpB+deltaT; searchSpace2(6,2)=LcpB;
                searchSpace2(7,1)=TcpB-deltaT; searchSpace2(7,2)=LcpB-deltaL1;
                searchSpace2(8,1)=TcpB; searchSpace2(8,2)=LcpB-deltaL1;
                searchSpace2(9,1)=TcpB+deltaT; searchSpace2(9,2)=LcpB-deltaL1;
                
                x1S1=T(1); y1S1=CS1(1)+CS1(2)*T(1);
                x2S3=T(end); y2S3=CS3(1)+CS3(2)*T(end);
                min_RMSE=100;
                for s1=1:9
                    x2S1=searchSpace1(s1,1); y2S1=searchSpace1(s1,2);
                    mS1=(y2S1-y1S1)/(x2S1-x1S1);
                    CS1(1)=y1S1-mS1*x1S1;
                    CS1(2)=mS1;
                    PLS1=CS1(1)+CS1(2).*TS1(:,2);
                    RMSE_S1 = sqrt(sum((PLS1 - LS1) .^ 2)/size(TS1,1));
                    for s2=1:9
                        x1S2=searchSpace1(s1,1); y1S2=searchSpace1(s1,2); x2S2=searchSpace2(s2,1); y2S2=searchSpace2(s2,2);
                        mS2=(y2S2-y1S2)/(x2S2-x1S2);
                        CS2(1)=y1S2-mS2*x1S2;
                        CS2(2)=mS2;
                        PLS2=CS2(1)+CS2(2).*TS2(:,2);
                        RMSE_S2 = sqrt(sum((PLS2 - LS2) .^ 2)/size(TS2,1));
                        
                        x1S3=searchSpace2(s2,1); y1S3=searchSpace2(s2,2); 
                        mS3=(y2S3-y1S3)/(x2S3-x1S3);
                        CS3(1)=y1S3-mS3*x1S3;
                        CS3(2)=mS3;
                        PLS3=CS3(1)+CS3(2).*TS3(:,2);
                        RMSE_S3 = sqrt(sum((PLS3 - LS3) .^ 2)/size(TS3,1));
                        
                        RMSE = RMSE_S1 + RMSE_S2 + RMSE_S3;
                        if(RMSE < min_RMSE)
                            min_RMSE = RMSE;
                            optS1x=x1S2;
                            optS2x=x2S2;
                            optS2y=y2S2;
                            optS1y=y1S2;
                            
                            optslope1=mS1;
                            optslope2=mS2;
                            optslope3=mS3;
                        end
                    end
                end
                
            end
         points =[optS1x,optS1y,optS2x,optS2y];
         slopes = [optslope1,optslope2,optslope3];
end
