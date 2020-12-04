package org.hisp.dhis.tracker.preheat.supplier.classStrategy;

/*
 * Copyright (c) 2004-2020, University of Oslo
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * Neither the name of the HISP project nor the names of its contributors may
 * be used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
 * ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.hisp.dhis.attribute.Attribute;
import org.hisp.dhis.common.IdentifiableObject;
import org.hisp.dhis.common.IdentifiableObjectManager;
import org.hisp.dhis.fieldfilter.Defaults;
import org.hisp.dhis.query.Query;
import org.hisp.dhis.query.QueryService;
import org.hisp.dhis.query.Restriction;
import org.hisp.dhis.query.Restrictions;
import org.hisp.dhis.schema.Schema;
import org.hisp.dhis.schema.SchemaService;
import org.hisp.dhis.tracker.TrackerIdScheme;
import org.hisp.dhis.tracker.TrackerIdentifier;
import org.hisp.dhis.tracker.TrackerImportParams;
import org.hisp.dhis.tracker.preheat.TrackerPreheat;
import org.hisp.dhis.tracker.preheat.cache.PreheatCacheService;
import org.hisp.dhis.tracker.preheat.mappers.CopyMapper;
import org.hisp.dhis.tracker.preheat.mappers.PreheatMapper;
import org.hisp.dhis.user.User;
import org.mapstruct.factory.Mappers;

/**
 * Abstract Tracker Preheat strategy that applies to strategies that employ the
 * generic {@link QueryService} to fetch data
 * 
 * @author Luciano Fiandesio
 */
public abstract class AbstractSchemaStrategy implements ClassBasedSupplierStrategy
{
    protected final SchemaService schemaService;

    private final QueryService queryService;

    private final IdentifiableObjectManager manager;

    private final PreheatCacheService cache;

    public AbstractSchemaStrategy( SchemaService schemaService, QueryService queryService,
        IdentifiableObjectManager manager, PreheatCacheService preheatCacheService )
    {
        this.schemaService = schemaService;
        this.queryService = queryService;
        this.manager = manager;
        this.cache = preheatCacheService;
    }

    @Override
    public void add( TrackerImportParams params, List<List<String>> splitList, TrackerPreheat preheat )
    {
        TrackerIdentifier identifier = params.getIdentifiers().getByClass( getSchemaClass() );
        Schema schema = schemaService.getDynamicSchema( getSchemaClass() );

        queryForIdentifiableObjects( preheat, schema, identifier, splitList, mapper() );
    }

    private Class<? extends PreheatMapper> mapper()
    {
        return getClass().getAnnotation( StrategyFor.class ).mapper();
    }

    private boolean canCache()
    {
        return getClass().getAnnotation( StrategyFor.class ).cache();
    }

    private int getCacheTTL()
    {
        return getClass().getAnnotation( StrategyFor.class ).ttl();
    }

    protected Class<?> getSchemaClass()
    {
        return getClass().getAnnotation( StrategyFor.class ).value();
    }

    @SuppressWarnings( "unchecked" )
    protected void queryForIdentifiableObjects( TrackerPreheat preheat, Schema schema, TrackerIdentifier identifier,
        List<List<String>> splitList, Class<? extends PreheatMapper> mapper )
    {

        TrackerIdScheme idScheme = identifier.getIdScheme();
        for ( List<String> ids : splitList )
        {
            List<? extends IdentifiableObject> objects;

            if ( TrackerIdScheme.ATTRIBUTE.equals( idScheme ) )
            {
                Attribute attribute = new Attribute();
                attribute.setUid( identifier.getValue() );
                objects = manager.getAllByAttributeAndValues(
                    (Class<? extends IdentifiableObject>) schema.getKlass(), attribute, ids );
            }
            else
            {
                objects = cacheAwareFetch( preheat.getUser(), schema, identifier, ids, mapper );
            }

            preheat.put( identifier, objects );
        }
    }

    @SuppressWarnings("unchecked")
    private <T extends IdentifiableObject> List<T> cacheAwareFetch( User user, Schema schema, TrackerIdentifier identifier, List<String> ids, Class<? extends PreheatMapper> mapper )
    {
        TrackerIdScheme idScheme = identifier.getIdScheme();
        
        List<T> objects;
        final String cacheKey = schema.getKlass().getSimpleName();
        
        if ( canCache() ) // check if this strategy requires caching
        {
            List<String> toRemove = new ArrayList<>();
            List<T> cachedObjects = new ArrayList<>();

            for ( String id : ids )
            {
                // is the object reference by the given id in cache?
                T o = cache.get( cacheKey, id );
                if ( o != null )
                {
                    // add to objects to set into preheat
                    cachedObjects.add( o );
                    // remove this id from list of id to fetch from db
                    toRemove.add( id );
                }
            }
            // is there any object which was not found in cache?
            if ( ids.size() > toRemove.size() )
            {
                // remove from the list of ids the ids found in cache
                ids.removeAll( toRemove );
                
                // execute the query
                objects = (List<T>) map( queryService.query( buildQuery( schema, user, idScheme, ids ) ), mapper );

                // put objects in query based on given scheme
                if ( idScheme.equals( TrackerIdScheme.UID ) )
                {
                    objects.forEach( o -> cache.put( cacheKey, o.getUid(), o, getCacheTTL() ) );
                }
                else if ( idScheme.equals( TrackerIdScheme.CODE ) )
                {
                    objects.forEach( o -> cache.put( cacheKey, o.getCode(), o, getCacheTTL() ) );
                }
                else if ( idScheme.equals( TrackerIdScheme.ATTRIBUTE ) )
                {
                    // TODO
                }
                // add back the cached objects to the final list
                objects.addAll( cachedObjects );
            }
            else
            {
                objects = cachedObjects;
            }
        }
        else
        {
            objects = (List<T>) map( queryService.query( buildQuery( schema, user, idScheme, ids ) ), mapper );
        }
            
        return objects;
    }
    
    private <T extends IdentifiableObject> List<T> map( List<T> objects, Class<? extends PreheatMapper> mapper )
    {

        if ( mapper.isAssignableFrom( CopyMapper.class ) )
        {
            return objects;
        }
        else
        {
            return (List<T>)objects.stream().map( o -> Mappers.getMapper( mapper ).map( o ) )
                .map( IdentifiableObject.class::cast ).collect( Collectors.toList() );
        }
    }
    

    private Query buildQuery( Schema schema, User user, TrackerIdScheme idScheme, List<String> ids )
    {
        Query query = Query.from( schema );
        query.setUser( user );
        query.add( generateRestrictionFromIdentifiers( idScheme, ids ) );
        query.setDefaults( Defaults.INCLUDE );

        return query;
    }

    private Restriction generateRestrictionFromIdentifiers( TrackerIdScheme idScheme, List<String> ids )
    {
        if ( TrackerIdScheme.CODE.equals( idScheme ) )
        {
            return Restrictions.in( "code", ids );
        }
        else
        {
            return Restrictions.in( "id", ids );
        }
    }
}