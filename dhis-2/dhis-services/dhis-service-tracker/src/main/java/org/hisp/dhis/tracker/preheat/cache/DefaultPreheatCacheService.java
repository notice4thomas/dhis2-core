package org.hisp.dhis.tracker.preheat.cache;

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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.hisp.dhis.common.IdentifiableObject;
import org.springframework.stereotype.Service;

/**
 * @author Luciano Fiandesio
 */
@Service
public class DefaultPreheatCacheService implements PreheatCacheService
{
    // TODO add Tracker Cache enable/disabled global setting

    private static Map<String, Cache<String, Object>> cache = new HashMap<>();

    @Override
    public <T extends IdentifiableObject> T get( String cacheKey, String id )
    {
        if ( cache.containsKey( cacheKey ) )
        {
            return (T) this.cache.get( cacheKey ).get( id );
        }

        return null;
    }

    @Override
    public <T extends IdentifiableObject> void put( String cacheKey, String uid, T o, int cacheTTL )
    {
        if ( o == null )
        {
            return;
        }
        if ( cache.containsKey( cacheKey ) )
        {
            cache.get( cacheKey ).put( uid, o );
        }
        else
        {
            Cache<String, Object> c = new Cache2kBuilder<String, Object>()
            {
            }
                .expireAfterWrite( cacheTTL, TimeUnit.MINUTES ) // expire/refresh after 5 minutes
                .name( cacheKey )
                .permitNullValues( false )
                .resilienceDuration( 30, TimeUnit.SECONDS ) // cope with at most 30 seconds
                // outage before propagating
                // exceptions
                .build();
            c.put( uid, o );
            cache.put( cacheKey, c );
        }
    }
}
