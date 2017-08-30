package com.hibernate.campaign.main;

import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

public class CreateSessionFactory {

	public static SessionFactory factory = null;
	
	public static SessionFactory getFactoryInstance(){
		
		if(null == factory){
		
			factory = new Configuration().configure().buildSessionFactory();
			
			return factory;
			
		}
		else{
			return factory;
		}
		
	}
	
}