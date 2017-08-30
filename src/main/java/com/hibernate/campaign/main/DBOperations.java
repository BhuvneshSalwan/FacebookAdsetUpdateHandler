package com.hibernate.campaign.main;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Expression;
import org.hibernate.criterion.Restrictions;

import com.hibernate.campaign.entities.FBCityCode;

public class DBOperations {

	private static DBOperations dao = null;
	
	public static DBOperations getObject(){
		
		if(null == dao){
			dao = new DBOperations();
			return dao;
		}
		else{
			return dao;
		}
		
	}
		
	public FBCityCode getCityCodeByName(String name) {
		Session session = null;
		try {
			session = CreateSessionFactory.getFactoryInstance().openSession();
			session.beginTransaction();
			Criteria cri = session.createCriteria(FBCityCode.class);
			cri.add(Expression.eq("city", name));
			List<FBCityCode> cityCodeEntities = cri.list();
			
			session.getTransaction().commit();
			
			if(cityCodeEntities.size() > 0){
				session.close();
				return cityCodeEntities.get(0);
			}
			else{
				session.close();
				return null;
			}
		
		} catch (Exception e) {
			session.close();
			return null;
		}
	}
	
}