#pragma once
/**
 * @file
 *
 * @brief Result type for explicit error handling with async operations.
 *
 * Based on boost::outcome for compatibility with kj-async coroutines.
 * This provides an alternative to exception-based error handling
 * that works well with C++20 coroutines and async operations.
 */

#include <boost/outcome/std_outcome.hpp>
#include <boost/outcome/std_result.hpp>
#include <boost/outcome/success_failure.hpp>
#include <exception>

namespace nix {

/**
 * A result type that holds either a value of type T or an exception.
 *
 * This is the primary error handling mechanism for async operations,
 * allowing errors to be propagated through promise chains without
 * throwing exceptions at suspension points.
 *
 * @tparam T The success value type
 * @tparam E The error type, defaults to std::exception_ptr
 */
template<typename T, typename E = std::exception_ptr>
using Result = boost::outcome_v2::std_result<T, E>;

/**
 * An outcome type that holds a value, an error, or a "domain" value.
 *
 * This is useful for operations that can succeed, fail with an error,
 * or return a special "partial success" or domain-specific result.
 *
 * @tparam T The success value type
 * @tparam D The domain value type
 * @tparam E The error type, defaults to std::exception_ptr
 */
template<typename T, typename D, typename E = std::exception_ptr>
using Outcome = boost::outcome_v2::std_outcome<T, D, E>;

/**
 * Namespace containing helper functions for constructing Result values.
 */
namespace result {

using boost::outcome_v2::failure;
using boost::outcome_v2::success;

/**
 * Create a Result containing the current exception.
 *
 * This is typically used in catch blocks to capture the exception
 * and return it as a Result:
 *
 * @code
 * try {
 *     return someOperation();
 * } catch (...) {
 *     return result::current_exception();
 * }
 * @endcode
 */
inline auto current_exception()
{
    return failure(std::current_exception());
}

} // namespace result

} // namespace nix
