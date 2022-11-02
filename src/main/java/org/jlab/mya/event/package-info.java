/**
 *         <p>Provides the event classes.</p>
 *         <p>Each Mya Event type has a separate class so that primitive values can be used to avoid the autoboxing
 *             overhead that would be associated with a generic Event.getValue() method.  Some event types such as
 *             AnalyzedFloatEvent and LabeledEnumEvent provide additional functionality beyond distinguishing between
 *             data types.
 *         </p>
 */
package org.jlab.mya.event;